"""Great Expectations batch-quality gate for fraud.gold.

Runs two server-side aggregate queries (databricks-sql-connector) against
fraud.gold.fct_reconciliation and fct_card_velocity_features, producing
seven statistics in one row. That row is the batch: one Expectation Suite,
one Checkpoint, ephemeral context (no stores, no Data Docs).

Exit code is the contract for the Week 3 Airflow gating task: 0 if every
expectation passed, 1 otherwise.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import great_expectations as gx
import pandas as pd
from databricks import sql as databricks_sql
from dotenv import load_dotenv

# The six recon_status values fct_reconciliation's case statement can ever
# produce (see dbt/models/gold/fct_reconciliation.sql `statused` CTE).
RECON_STATUSES = (
    "AUTH_DECLINED",
    "AMOUNT_MISMATCH",
    "MATCHED",
    "AUTH_NO_SETTLEMENT",
    "PENDING_SETTLEMENT",
    "ORPHAN_SETTLEMENT",
)
_RECON_STATUS_SQL_LIST = ", ".join(f"'{status}'" for status in RECON_STATUSES)

RECON_METRICS_SQL = f"""
select
    count(*) as recon_row_count,
    sum(case when recon_status not in ({_RECON_STATUS_SQL_LIST}) then 1 else 0 end)
        as recon_status_invalid_count,
    sum(case when is_matured and recon_status = 'MATCHED' then 1 else 0 end)
        / nullif(sum(case when is_matured then 1 else 0 end), 0) as matched_share_of_matured,
    sum(case when recon_status = 'ORPHAN_SETTLEMENT' then 1 else 0 end)
        / nullif(count(*), 0) as orphan_settlement_share
from fraud.gold.fct_reconciliation
"""

GATE_RESULTS_TABLE = "fraud.gold.ge_gate_results"

# One row per expectation per run, so the dashboard can show both the current
# verdict and how quality moved over time.
CREATE_RESULTS_TABLE_SQL = f"""
create table if not exists {GATE_RESULTS_TABLE} (
    checked_at timestamp,
    expectation string,
    description string,
    observed double,
    passed boolean,
    overall_success boolean
)
"""

VELOCITY_METRICS_SQL = """
select
    avg(cast(is_fraud as double)) as fraud_rate,
    percentile_approx(case when is_fraud = 0 then implied_speed_kmh end, 0.99)
        as p99_implied_speed_kmh_nonfraud,
    sum(case when txn_count_24h < txn_count_1h then 1 else 0 end) as velocity_inversion_count
from fraud.gold.fct_card_velocity_features
"""

# (metrics column, description, expectation). Every check is a bounds check
# on one column, including the two count-based ones (== 0 / >= 1).
EXPECTATIONS = [
    (
        "recon_row_count",
        "fct_reconciliation row count > 0",
        gx.expectations.ExpectColumnValuesToBeBetween(column="recon_row_count", min_value=1),
    ),
    (
        "recon_status_invalid_count",
        "recon_status values within the six-status set",
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="recon_status_invalid_count", min_value=0, max_value=0
        ),
    ),
    (
        "matched_share_of_matured",
        "MATCHED share of matured rows in [0.60, 0.98]",
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="matched_share_of_matured", min_value=0.60, max_value=0.98
        ),
    ),
    (
        "orphan_settlement_share",
        "ORPHAN_SETTLEMENT share of all rows < 0.03",
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="orphan_settlement_share", min_value=0, max_value=0.03, strict_max=True
        ),
    ),
    (
        "fraud_rate",
        "fct_card_velocity_features fraud rate in [0.005, 0.05]",
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="fraud_rate", min_value=0.005, max_value=0.05
        ),
    ),
    (
        "p99_implied_speed_kmh_nonfraud",
        "p99 implied_speed_kmh (non-fraud rows) < 900",
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="p99_implied_speed_kmh_nonfraud", min_value=0, max_value=900, strict_max=True
        ),
    ),
    (
        "velocity_inversion_count",
        "zero rows with txn_count_24h < txn_count_1h",
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="velocity_inversion_count", min_value=0, max_value=0
        ),
    ),
]


def _connect():
    return databricks_sql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"],
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
    )


def fetch_metrics_row() -> pd.DataFrame:
    with _connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(RECON_METRICS_SQL)
            recon = dict(zip((c[0] for c in cursor.description), cursor.fetchone()))

            cursor.execute(VELOCITY_METRICS_SQL)
            velocity = dict(zip((c[0] for c in cursor.description), cursor.fetchone()))

    return pd.DataFrame([{**recon, **velocity}])


def run_checkpoint(metrics_df: pd.DataFrame):
    context = gx.get_context(mode="ephemeral")

    data_source = context.data_sources.add_pandas(name="gold_quality_metrics_source")
    data_asset = data_source.add_dataframe_asset(name="gold_quality_metrics")
    batch_definition = data_asset.add_batch_definition_whole_dataframe(name="whole_row")

    suite = context.suites.add(gx.ExpectationSuite(name="gold_batch_quality_suite"))
    for _, _, expectation in EXPECTATIONS:
        suite.add_expectation(expectation)

    validation_definition = context.validation_definitions.add(
        gx.ValidationDefinition(
            name="gold_batch_quality_validation", data=batch_definition, suite=suite
        )
    )

    checkpoint = context.checkpoints.add(
        gx.Checkpoint(
            name="gold_batch_quality_checkpoint", validation_definitions=[validation_definition]
        )
    )

    return checkpoint.run(batch_parameters={"dataframe": metrics_df})


def success_by_column(checkpoint_result) -> dict[str, bool]:
    validation_result = next(iter(checkpoint_result.run_results.values()))
    return {
        result.expectation_config.kwargs["column"]: result.success
        for result in validation_result.results
    }


def print_summary(checkpoint_result, metrics_df: pd.DataFrame) -> bool:
    passed = success_by_column(checkpoint_result)

    row = metrics_df.iloc[0]
    for column, description, _ in EXPECTATIONS:
        status = "PASS" if passed.get(column) else "FAIL"
        print(f"[{status}] {description} (observed={row[column]!r})")

    return bool(checkpoint_result.success)


def record_results(checkpoint_result, metrics_df: pd.DataFrame, success: bool) -> None:
    """Append this run's verdict to fraud.gold.ge_gate_results.

    Without this the gate's result exists only as an exit code that Airflow
    consumes and discards - the dashboard has nothing to read and there's no
    history of when quality drifted. Self-bootstrapping (no dbt model), same
    precedent as the notebook-managed isolation_forest_scored_sample.
    """
    passed = success_by_column(checkpoint_result)
    row = metrics_df.iloc[0]
    checked_at = datetime.now(timezone.utc).isoformat()

    values = []
    for column, description, _ in EXPECTATIONS:
        observed = row[column]
        values.append([
            checked_at,
            column,
            description,
            None if pd.isna(observed) else float(observed),
            bool(passed.get(column)),
            success,
        ])

    placeholders = ", ".join(["(cast(? as timestamp), ?, ?, ?, ?, ?)"] * len(values))
    with _connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(CREATE_RESULTS_TABLE_SQL)
            cursor.execute(
                f"insert into {GATE_RESULTS_TABLE} "
                "(checked_at, expectation, description, observed, passed, overall_success) "
                f"values {placeholders}",
                [v for value_row in values for v in value_row],
            )


def main() -> int:
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
    metrics_df = fetch_metrics_row()
    checkpoint_result = run_checkpoint(metrics_df)
    success = print_summary(checkpoint_result, metrics_df)

    # Never let a logging failure change the verdict - the exit code is the
    # Airflow gating contract, and a warehouse hiccup while recording history
    # must not turn a passing batch into a failed task.
    try:
        record_results(checkpoint_result, metrics_df, success)
    except Exception as exc:
        print(f"[WARN] could not record results to {GATE_RESULTS_TABLE}: {exc}")

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
