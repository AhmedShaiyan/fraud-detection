"""Bronze -> Silver -> Gold via dbt, gated by Great Expectations.

Triggered by ingest_transactions updating fraud.bronze.transactions_raw, not
its own cron - avoids rebuilding Gold mid-ingest from a half-loaded Bronze
(dbt succeeds silently on stale data, so that failure mode is wrong numbers,
not a red task).

Steps: run_dbt_build (models + tests, snapshots excluded) -> run_ge_gate
(nonzero exit fails) -> run_dbt_snapshot (merchant SCD2). Snapshot runs last
and only on success because it's an irreversible append - a bad batch
corrupts dbt_valid_from/to in a way no re-run undoes, unlike the idempotent
build.
"""

from __future__ import annotations

import os
from datetime import timedelta

from airflow.decorators import dag
from airflow.providers.standard.operators.bash import BashOperator

from assets import BRONZE_TRANSACTIONS_RAW

DBT_DIR = "/opt/airflow/dbt"
# Own venvs, not Airflow's - pins conflict with databricks-sdk/-sql-connector
# versions the ingest DAG needs. See airflow/Dockerfile.
DBT_BIN = "/opt/dbt-venv/bin/dbt"
GE_PYTHON = "/opt/ge-venv/bin/python"

# Repo is read-only, so dbt's writable paths are redirected into the staging
# mount. BashOperator(env=...) replaces the child env wholesale, so the
# Databricks vars profiles.yml needs via env_var() are passed explicitly.
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
    # under backfill, dbt+GE can outlast the 15-min ingest cadence; queue
    # rather than stack warehouse load.
    max_active_runs=1,
    default_args={"retries": 0},
    tags=["transform", "quality", "dbt"],
    doc_md=__doc__,
)
def transform_quality():
    run_dbt_build = BashOperator(
        task_id="run_dbt_build",
        # snapshot excluded: see module docstring
        bash_command=(
            f"{DBT_BIN} build --project-dir {DBT_DIR} --profiles-dir {DBT_DIR} "
            "--exclude-resource-type snapshot"
        ),
        env=DBT_ENV,
        # only network-crossing task worth retrying on a dropped connection
        retries=2,
        retry_delay=timedelta(seconds=30),
        retry_exponential_backoff=True,
    )

    run_ge_gate = BashOperator(
        task_id="run_ge_gate",
        # exit code contract: 0 all-pass, 1 otherwise
        bash_command=f"{GE_PYTHON} /opt/airflow/quality/ge_checkpoint.py",
        # no retries: a failed expectation is a verdict, not a flaky call
        retries=0,
    )

    # default trigger_rule (all_success): a failed gate skips this, no SCD2 append
    run_dbt_snapshot = BashOperator(
        task_id="run_dbt_snapshot",
        bash_command=f"{DBT_BIN} snapshot --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}",
        env=DBT_ENV,
        retries=0,
    )

    run_dbt_build >> run_ge_gate >> run_dbt_snapshot


transform_quality()
