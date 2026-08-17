"""Streamlit dashboard over the fraud lakehouse.

Read-only window onto Gold: reconciliation health, detection performance,
what's currently flagged, and whether the last quality gate passed.

Quota discipline: every panel is served from one cached round trip. A page
load opens a single warehouse connection, runs the six aggregate queries, and
caches the frames indefinitely - idle viewing costs nothing, and nothing
re-queries until Refresh is pressed.
"""

from __future__ import annotations

import os

import altair as alt
import pandas as pd
import streamlit as st
from databricks import sql as databricks_sql

import queries

# Validated with the dataviz palette validator against a white surface
# (all-pairs): categorical trio worst CVD dE 9.2 / normal-vision 24.0; the
# break pair worst CVD dE 23.8. Aqua sits at 2.82:1 contrast, so panel 2 ships
# the table alongside the chart as the required relief.
BLUE, ORANGE, AQUA = "#2a78d6", "#eb6834", "#1baf7a"
CRITICAL, GOOD = "#d03b3b", "#0ca30c"
STRATEGY_COLORS = [BLUE, ORANGE, AQUA]

st.set_page_config(page_title="Fraud pipeline", page_icon="*", layout="wide")


@st.cache_data(show_spinner="Querying the warehouse...")
def load_all() -> dict[str, pd.DataFrame]:
    """One connection, six queries. The warehouse bills wakeups more than
    statements, so they share a connection rather than opening six."""
    with databricks_sql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"],
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
    ) as conn:
        ge_latest, ge_history = queries.ge_gate_status(conn)
        return {
            "recon": queries.recon_distribution(conn),
            "recall": queries.recall_by_type(conn),
            "overall": queries.overall_metrics(conn),
            "flagged": queries.recent_flagged(conn),
            "volume": queries.daily_volume(conn),
            "quarantine": queries.quarantine_rate(conn),
            "ge_latest": ge_latest,
            "ge_history": ge_history,
        }


def _numeric(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Warehouse decimals arrive as Decimal, which Altair can't encode."""
    out = df.copy()
    for col in columns:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


# sidebar

with st.sidebar:
    st.header("Controls")
    if st.button("Refresh data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    # Off by default - each refresh is a warehouse wakeup against a fair-use
    # quota, so polling is opt-in rather than the default posture.
    auto = st.toggle("Auto-refresh", value=False)
    interval = st.select_slider("Interval", [30, 60, 300, 900], value=300, disabled=not auto)
    st.caption("Auto-refresh is off by default: every refresh wakes the warehouse.")

if auto:

    @st.fragment(run_every=interval)
    def _auto_refresh() -> None:
        st.cache_data.clear()
        st.rerun(scope="app")

    _auto_refresh()

st.title("Fraud detection pipeline")

try:
    data = load_all()
except Exception as exc:
    st.error(f"Could not reach the SQL warehouse: {type(exc).__name__}: {exc}")
    st.stop()


# panel 1: reconciliation

st.header("1 - Reconciliation status")

recon = _numeric(data["recon"], ["n"])
if recon.empty:
    st.info("fct_reconciliation is empty.")
else:
    recon["Outcome"] = recon["is_break"].map({True: "Break", False: "Clean"})
    recon["Maturity"] = recon["is_matured"].map({True: "Matured", False: "Pending maturity"})

    total, breaks = int(recon["n"].sum()), int(recon.loc[recon["is_break"], "n"].sum())
    left, mid, right = st.columns(3)
    left.metric("Recon rows", f"{total:,}")
    mid.metric("Breaks", f"{breaks:,}")
    right.metric("Break rate", f"{breaks / total:.1%}" if total else "n/a")

    # Faceted rather than stacked: matured and pending answer different
    # questions (a real break vs. a settlement that simply hasn't landed),
    # and stacking them would imply they're comparable parts of one whole.
    chart = (
        alt.Chart(recon)
        .mark_bar(cornerRadiusEnd=4, height={"band": 0.7})
        .encode(
            x=alt.X("n:Q", title="Transactions", axis=alt.Axis(grid=True, gridOpacity=0.25)),
            y=alt.Y("recon_status:N", title=None, sort="-x"),
            color=alt.Color(
                "Outcome:N",
                scale=alt.Scale(domain=["Clean", "Break"], range=[BLUE, CRITICAL]),
                legend=alt.Legend(title=None, orient="top"),
            ),
            tooltip=["recon_status", "Outcome", "Maturity", alt.Tooltip("n:Q", format=",")],
        )
        .properties(height=200)
        .facet(column=alt.Column("Maturity:N", title=None, sort=["Matured", "Pending maturity"]))
        .resolve_scale(x="independent", y="independent")
    )
    st.altair_chart(chart, use_container_width=True)


# panel 2: detection performance

st.header("2 - Fraud detection performance")

recall = _numeric(
    data["recall"], ["n_fraud", "recall_rules_only", "recall_model_only", "recall_hybrid"]
)
if recall.empty:
    st.info("isolation_forest_scored_sample is empty - run the training notebook.")
else:
    table = recall.rename(
        columns={
            "fraud_type": "Fraud type",
            "n_fraud": "Fraud rows",
            "recall_rules_only": "Rules only",
            "recall_model_only": "Model only",
            "recall_hybrid": "Hybrid",
        }
    )
    st.dataframe(
        table.style.format(
            {"Rules only": "{:.3f}", "Model only": "{:.3f}", "Hybrid": "{:.3f}", "Fraud rows": "{:,}"}
        ),
        hide_index=True,
        use_container_width=True,
    )

    long = table.melt(
        id_vars="Fraud type",
        value_vars=["Rules only", "Model only", "Hybrid"],
        var_name="Strategy",
        value_name="Recall",
    )
    st.altair_chart(
        alt.Chart(long)
        .mark_bar(cornerRadiusEnd=4)
        .encode(
            x=alt.X("Strategy:N", title=None, axis=alt.Axis(labels=False, ticks=False)),
            y=alt.Y("Recall:Q", scale=alt.Scale(domain=[0, 1]),
                    axis=alt.Axis(format=".0%", grid=True, gridOpacity=0.25)),
            color=alt.Color(
                "Strategy:N",
                scale=alt.Scale(domain=["Rules only", "Model only", "Hybrid"], range=STRATEGY_COLORS),
                legend=alt.Legend(title=None, orient="top"),
            ),
            column=alt.Column("Fraud type:N", title=None),
            tooltip=["Fraud type", "Strategy", alt.Tooltip("Recall:Q", format=".3f")],
        )
        .properties(height=240, width=110),
        use_container_width=False,
    )

    overall = _numeric(data["overall"], ["precision", "recall", "fpr"])
    st.subheader("Overall precision / recall / FPR")
    st.dataframe(
        overall.rename(
            columns={
                "strategy": "Strategy",
                "precision": "Precision",
                "recall": "Recall",
                "fpr": "FPR",
                "true_positives": "TP",
                "false_positives": "FP",
            }
        ).style.format({"Precision": "{:.3f}", "Recall": "{:.3f}", "FPR": "{:.4f}"}),
        hide_index=True,
        use_container_width=True,
    )
    st.caption(
        "Recall above is exact - the scored sample contains every holdout fraud row. "
        "**Precision and FPR are not comparable to the notebook's figures**: the sample "
        "keeps only ~1000 sampled non-fraud rows, so false positives are undersampled, "
        "inflating precision and deflating FPR. The notebook's MLflow run holds the "
        "true full-holdout values."
    )


# panel 3: recent flagged

st.header("3 - Recent flagged transactions")

flagged = _numeric(data["flagged"], ["amount", "anomaly_score"])
if flagged.empty:
    st.info("Nothing flagged in the scored sample.")
else:
    rule_cols = [name.lower() for name in queries.RULES]

    def _fired(row) -> str:
        hits = [c.replace("_rule", "") for c in rule_cols if bool(row.get(c))]
        return ", ".join(hits) if hits else "-"

    view = pd.DataFrame({
        "Time": flagged["event_time"],
        "Card": flagged["card_id_hash"].str.slice(0, 12),
        "Amount": flagged["amount"],
        "Anomaly score": flagged["anomaly_score"],
        "Model": flagged["model_flag"].map({True: "flag", False: "-"}),
        "Rules fired": flagged.apply(_fired, axis=1),
        # tinyint 0/1, not boolean - see queries.py
        "Actual fraud": flagged["is_fraud"].astype(bool).map({True: "yes", False: "no"}),
        "Fraud type": flagged["fraud_type"].fillna("-"),
    })
    st.dataframe(
        view.style.format({"Amount": "{:,.2f}", "Anomaly score": "{:.4f}"}),
        hide_index=True,
        use_container_width=True,
        height=420,
    )
    st.caption(
        "Rule columns are evaluated from api/scoring.py's RULES, the same thresholds "
        "the model notebook and the FastAPI scorer use. 'Actual fraud' is ground truth, "
        "available here only because the data is synthetic."
    )


# panel 4: volume and quality

st.header("4 - Volume and quality")

vol_col, quar_col = st.columns(2)

with vol_col:
    st.subheader("Daily events by type")
    volume = _numeric(data["volume"], ["n"])
    if volume.empty:
        st.info("No silver events yet.")
    else:
        st.altair_chart(
            alt.Chart(volume)
            .mark_line(strokeWidth=2, point=alt.OverlayMarkDef(size=45))
            .encode(
                x=alt.X("event_date:T", title=None),
                y=alt.Y("n:Q", title="Events", axis=alt.Axis(grid=True, gridOpacity=0.25)),
                color=alt.Color(
                    "event_type:N",
                    scale=alt.Scale(domain=["AUTH", "SETTLEMENT"], range=[BLUE, ORANGE]),
                    legend=alt.Legend(title=None, orient="top"),
                ),
                tooltip=["event_date:T", "event_type:N", alt.Tooltip("n:Q", format=",")],
            )
            .properties(height=260),
            use_container_width=True,
        )

with quar_col:
    st.subheader("Quarantine rate")
    quarantine = _numeric(data["quarantine"], ["quarantined_rows", "total_rows", "quarantine_rate"])
    if quarantine.empty:
        st.info("No quarantine history yet.")
    else:
        # Separate chart from volume rather than a second y-axis on the same
        # plot: a rate and a count share no scale, and dual axes invite false
        # correlations.
        st.altair_chart(
            alt.Chart(quarantine)
            .mark_line(strokeWidth=2, point=alt.OverlayMarkDef(size=45), color=BLUE)
            .encode(
                x=alt.X("event_date:T", title=None),
                y=alt.Y("quarantine_rate:Q", title="Quarantined share",
                        axis=alt.Axis(format=".2%", grid=True, gridOpacity=0.25)),
                tooltip=[
                    "event_date:T",
                    alt.Tooltip("quarantine_rate:Q", format=".3%"),
                    alt.Tooltip("quarantined_rows:Q", format=","),
                    alt.Tooltip("total_rows:Q", format=","),
                ],
            )
            .properties(height=260),
            use_container_width=True,
        )
        st.caption(
            "By ingestion date, not event date: a malformed event_time is itself a "
            "quarantine reason, so keying on it would drop the rows being counted."
        )

st.subheader("Great Expectations gate")

ge_latest, ge_history = data["ge_latest"], data["ge_history"]
if ge_latest.empty:
    st.info(
        "No gate runs recorded yet. fraud.gold.ge_gate_results is created by "
        "quality/ge_checkpoint.py on its first run."
    )
else:
    overall_ok = bool(ge_latest["overall_success"].iloc[0])
    checked_at = ge_latest["checked_at"].iloc[0]
    failed = int((~ge_latest["passed"].astype(bool)).sum())

    # Icon + label, never colour alone - status colour is the third cue here.
    if overall_ok:
        st.success(f"PASS - all {len(ge_latest)} expectations passed (last run {checked_at} UTC)")
    else:
        st.error(f"FAIL - {failed} of {len(ge_latest)} expectations failed (last run {checked_at} UTC)")

    detail = _numeric(ge_latest, ["observed"])
    st.dataframe(
        pd.DataFrame({
            "Result": detail["passed"].map({True: "PASS", False: "FAIL"}),
            "Expectation": detail["description"],
            "Observed": detail["observed"],
        }).style.format({"Observed": "{:.6g}"}),
        hide_index=True,
        use_container_width=True,
    )

    if not ge_history.empty and len(ge_history) > 1:
        history = ge_history.copy()
        history["Status"] = history["overall_success"].map({True: "Pass", False: "Fail"})
        st.altair_chart(
            alt.Chart(history)
            .mark_bar(cornerRadiusEnd=4, size=14)
            .encode(
                x=alt.X("checked_at:T", title=None),
                y=alt.Y("failed_expectations:Q", title="Failed expectations",
                        axis=alt.Axis(tickMinStep=1, grid=True, gridOpacity=0.25)),
                color=alt.Color(
                    "Status:N",
                    scale=alt.Scale(domain=["Pass", "Fail"], range=[GOOD, CRITICAL]),
                    legend=alt.Legend(title=None, orient="top"),
                ),
                tooltip=["checked_at:T", "Status:N", "failed_expectations:Q"],
            )
            .properties(height=200),
            use_container_width=True,
        )
