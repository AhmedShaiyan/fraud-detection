"""SQL for the dashboard, kept apart from layout.

Every statement aggregates server-side. The one exception is recent_flagged,
which is row-level but capped at 50 - Free Edition's warehouse is a shared
fair-use resource, and pulling holdout-sized frames into a Streamlit process
would burn quota for nothing.
"""

from __future__ import annotations

import pandas as pd

# Thresholds come from the serving contract, not from literals typed here.
# notebook -> api/scoring.py -> dashboard stays one source of truth, and
# api/tests/test_scoring.py keeps guarding it.
from scoring import RULES

GATE_RESULTS_TABLE = "fraud.gold.ge_gate_results"


def _query(conn, sql: str) -> pd.DataFrame:
    with conn.cursor() as cursor:
        cursor.execute(sql)
        columns = [c[0] for c in cursor.description]
        return pd.DataFrame(cursor.fetchall(), columns=columns)


# --- panel 1: reconciliation ------------------------------------------------

RECON_DISTRIBUTION_SQL = """
select recon_status, is_break, is_matured, count(*) as n
from fraud.gold.fct_reconciliation
group by 1, 2, 3
order by n desc
"""


def recon_distribution(conn) -> pd.DataFrame:
    return _query(conn, RECON_DISTRIBUTION_SQL)


# --- panel 2: detection performance -----------------------------------------

# Recall per fraud_type for each of the three strategies. Every holdout fraud
# row is in the sample, so these reproduce the notebook's figures exactly.
RECALL_BY_TYPE_SQL = """
select
    fraud_type,
    count(*) as n_fraud,
    avg(case when rule_flag then 1.0 else 0.0 end) as recall_rules_only,
    avg(case when predicted_fraud then 1.0 else 0.0 end) as recall_model_only,
    avg(case when combined_flag then 1.0 else 0.0 end) as recall_hybrid
from fraud.gold.isolation_forest_scored_sample
-- is_fraud is tinyint (0/1), not boolean - the three *_flag columns are
-- genuine booleans, so only this one needs the comparison.
where is_fraud = 1
group by 1
order by 1
"""

# Precision/recall/FPR per strategy. Unlike recall-by-type these are NOT
# comparable to the notebook: the sample holds every fraud row but only 1000
# sampled non-fraud rows, so false positives are undersampled - precision reads
# high and FPR low. Surfaced with an explicit caption in app.py.
OVERALL_METRICS_SQL = """
with flags as (
    select
        -- normalise the tinyint label to boolean once, here
        is_fraud = 1 as is_fraud,
        rule_flag as rules_only,
        predicted_fraud as model_only,
        combined_flag as hybrid
    from fraud.gold.isolation_forest_scored_sample
)
select
    stack(3,
        'rules-only', sum(case when rules_only and is_fraud then 1 else 0 end),
                      sum(case when rules_only and not is_fraud then 1 else 0 end),
        'model-only', sum(case when model_only and is_fraud then 1 else 0 end),
                      sum(case when model_only and not is_fraud then 1 else 0 end),
        'hybrid',     sum(case when hybrid and is_fraud then 1 else 0 end),
                      sum(case when hybrid and not is_fraud then 1 else 0 end)
    ) as (strategy, true_positives, false_positives),
    sum(case when is_fraud then 1 else 0 end) as total_fraud,
    sum(case when not is_fraud then 1 else 0 end) as total_nonfraud
from flags
"""


def recall_by_type(conn) -> pd.DataFrame:
    return _query(conn, RECALL_BY_TYPE_SQL)


def overall_metrics(conn) -> pd.DataFrame:
    df = _query(conn, OVERALL_METRICS_SQL)
    if df.empty:
        return df
    tp, fp = df["true_positives"].astype(float), df["false_positives"].astype(float)
    df["precision"] = tp / (tp + fp).replace(0, pd.NA)
    df["recall"] = tp / df["total_fraud"].astype(float).replace(0, pd.NA)
    df["fpr"] = fp / df["total_nonfraud"].astype(float).replace(0, pd.NA)
    return df[["strategy", "precision", "recall", "fpr", "true_positives", "false_positives"]]


# --- panel 3: recent flagged ------------------------------------------------


def _rule_columns_sql() -> str:
    """One boolean column per RULES entry, built from the shared thresholds."""
    return ",\n    ".join(
        f"f.{spec['column']} > {spec['threshold']} as {name.lower()}"
        for name, spec in RULES.items()
    )


def recent_flagged_sql(limit: int = 50) -> str:
    return f"""
select
    s.transaction_id,
    s.event_time,
    s.card_id_hash,
    s.amount,
    s.anomaly_score,
    s.predicted_fraud as model_flag,
    {_rule_columns_sql()},
    s.fraud_type,
    s.is_fraud
from fraud.gold.isolation_forest_scored_sample s
left join fraud.gold.fct_card_velocity_features f using (transaction_id)
where s.combined_flag
order by s.event_time desc
limit {limit}
"""


def recent_flagged(conn, limit: int = 50) -> pd.DataFrame:
    return _query(conn, recent_flagged_sql(limit))


# --- panel 4: volume and quality --------------------------------------------

# Silver rather than bronze: deduped, so counts reflect distinct events.
DAILY_VOLUME_SQL = """
select date(event_time) as event_date, event_type, count(*) as n
from (
    select event_time, event_type from fraud.silver.slv_authorizations
    union all
    select event_time, event_type from fraud.silver.slv_settlements
)
group by 1, 2
order by 1
"""

# Quarantined rows never reach the silver fact tables, so the denominator is
# clean + quarantined, not silver alone.
#
# Keyed on _ingested_at, not event_time. slv_quarantine.event_time is a STRING
# holding the raw value, and a malformed timestamp ('2026-13-45 99:99:99') is
# itself a quarantine reason - date(event_time) errors outright, and try_cast
# would silently drop exactly the rows this chart is meant to count. Ingestion
# time is always valid and gives numerator and denominator one time base.
QUARANTINE_RATE_SQL = """
with clean as (
    select date(_ingested_at) as event_date, count(*) as clean_rows
    from (
        select _ingested_at from fraud.silver.slv_authorizations
        union all
        select _ingested_at from fraud.silver.slv_settlements
    )
    group by 1
),
quarantined as (
    select date(_ingested_at) as event_date, count(*) as quarantined_rows
    from fraud.silver.slv_quarantine
    group by 1
)
select
    coalesce(c.event_date, q.event_date) as event_date,
    coalesce(q.quarantined_rows, 0) as quarantined_rows,
    coalesce(c.clean_rows, 0) + coalesce(q.quarantined_rows, 0) as total_rows,
    coalesce(q.quarantined_rows, 0)
        / nullif(coalesce(c.clean_rows, 0) + coalesce(q.quarantined_rows, 0), 0)
        as quarantine_rate
from clean c
full outer join quarantined q on c.event_date = q.event_date
order by 1
"""

GE_LATEST_SQL = f"""
select checked_at, expectation, description, observed, passed, overall_success
from {GATE_RESULTS_TABLE}
where checked_at = (select max(checked_at) from {GATE_RESULTS_TABLE})
order by expectation
"""

GE_HISTORY_SQL = f"""
select checked_at, max(overall_success) as overall_success,
       sum(case when passed then 0 else 1 end) as failed_expectations
from {GATE_RESULTS_TABLE}
group by checked_at
order by checked_at desc
limit 20
"""


def daily_volume(conn) -> pd.DataFrame:
    return _query(conn, DAILY_VOLUME_SQL)


def quarantine_rate(conn) -> pd.DataFrame:
    return _query(conn, QUARANTINE_RATE_SQL)


def ge_gate_status(conn) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Latest verdict plus recent history.

    Returns empty frames when the table doesn't exist yet - the gate creates it
    on its first run, so a fresh environment legitimately has nothing here.
    """
    try:
        return _query(conn, GE_LATEST_SQL), _query(conn, GE_HISTORY_SQL)
    except Exception:
        return pd.DataFrame(), pd.DataFrame()
