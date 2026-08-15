"""Bronze -> Silver -> Gold via dbt, gated by Great Expectations.

Runs when ingest_transactions updates fraud.bronze.transactions_raw - not on
its own cron. Two independent 15-minute schedules would eventually let this
DAG fire mid-ingest and rebuild Gold from a half-loaded Bronze, and it would
do so *silently*: dbt succeeds happily on stale data, so the failure mode is
wrong numbers, not a red task. The asset dependency makes ingest-before-
transform something the scheduler enforces rather than something the two
cron expressions happen to agree on.

Task order and why the snapshot is separated from the build:
  1. run_dbt_build   - models + tests, snapshots excluded.
  2. run_ge_gate     - aggregate expectations over Gold; nonzero exit fails.
  3. run_dbt_snapshot - merchant SCD2, only reached if the gate passed.

`dbt build` normally includes snapshots, which would run them in step 1,
ungated. They're excluded there and run last because every other model here
is an idempotent rebuild while a snapshot is an irreversible append to SCD2
history: a bad batch in fraud.gold.* is fixed by re-running dbt, but a bad
batch written into merchants_snapshot corrupts dbt_valid_from/dbt_valid_to
in a way no re-run undoes. The non-idempotent step is the one that waits
behind the quality gate.
"""

from __future__ import annotations

import os
from datetime import timedelta

from airflow.decorators import dag
from airflow.providers.standard.operators.bash import BashOperator

from assets import BRONZE_TRANSACTIONS_RAW

DBT_DIR = "/opt/airflow/dbt"
# Own venvs, not Airflow's - their pins conflict irreconcilably with the
# databricks-sdk/-sql-connector versions the ingest DAG needs. See
# airflow/Dockerfile.
DBT_BIN = "/opt/dbt-venv/bin/dbt"
GE_PYTHON = "/opt/ge-venv/bin/python"

# The repo is mounted read-only, so dbt's three write paths are redirected
# into the writable staging mount: target/ + partial_parse.msgpack via
# DBT_TARGET_PATH, logs/ via DBT_LOG_PATH, and .user.yml (written to the
# profiles dir on first run) suppressed by turning off anonymous stats.
# BashOperator(env=...) replaces the child environment wholesale, so the
# Databricks vars profiles.yml resolves via env_var() are passed through
# explicitly rather than inherited.
DBT_ENV = {
    "PATH": os.environ["PATH"],
    "HOME": os.environ.get("HOME", "/home/airflow"),
    "DBT_TARGET_PATH": "/opt/airflow/staging/dbt/target",
    "DBT_LOG_PATH": "/opt/airflow/staging/dbt/logs",
    "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
    "DATABRICKS_HOST": os.environ["DATABRICKS_HOST"],
    "DATABRICKS_HTTP_PATH": os.environ["DATABRICKS_HTTP_PATH"],
    "DATABRICKS_TOKEN": os.environ["DATABRICKS_TOKEN"],
}


@dag(
    dag_id="transform_quality",
    schedule=[BRONZE_TRANSACTIONS_RAW],
    catchup=False,
    # dbt build + GE against serverless outlasts the 15-minute ingest cadence
    # under backfill; queue the next run instead of stacking warehouse load.
    max_active_runs=1,
    default_args={"retries": 0},
    tags=["transform", "quality", "dbt"],
    doc_md=__doc__,
)
def transform_quality():
    run_dbt_build = BashOperator(
        task_id="run_dbt_build",
        # --exclude-resource-type snapshot: see module docstring.
        bash_command=(
            f"{DBT_BIN} build --project-dir {DBT_DIR} --profiles-dir {DBT_DIR} "
            "--exclude-resource-type snapshot"
        ),
        env=DBT_ENV,
        # The only network-crossing task worth retrying - a dropped warehouse
        # connection is transient in a way the other two aren't.
        retries=2,
        retry_delay=timedelta(seconds=30),
        retry_exponential_backoff=True,
    )

    run_ge_gate = BashOperator(
        task_id="run_ge_gate",
        # Exit code is the gate's documented contract: 0 all-pass, 1 otherwise.
        bash_command=f"{GE_PYTHON} /opt/airflow/quality/ge_checkpoint.py",
        # No retries by design. A failed expectation is a verdict about the
        # data, not a flaky call; retrying re-asks a question already answered.
        retries=0,
    )

    # Default trigger_rule (all_success) is exactly "only if the gate passed" -
    # a failed gate leaves this upstream_failed, so no SCD2 rows are appended.
    run_dbt_snapshot = BashOperator(
        task_id="run_dbt_snapshot",
        bash_command=f"{DBT_BIN} snapshot --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}",
        env=DBT_ENV,
        retries=0,
    )

    run_dbt_build >> run_ge_gate >> run_dbt_snapshot


transform_quality()
