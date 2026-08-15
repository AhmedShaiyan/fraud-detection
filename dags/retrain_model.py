"""Weekly Isolation Forest retrain on Databricks, Sundays 02:00 UTC.

Triggers the training notebook as a Databricks job and reports where the
resulting MLflow run landed. Training itself is a Databricks concern - the
notebook needs spark and dbutils, so it can only run there; Airflow's job is
to decide *when* and to surface the outcome.

A bare cron string here, deliberately unlike ingest_transactions'
CronDataIntervalTimetable. Airflow 3 resolves a bare string to
CronTriggerTimetable, where data_interval_start == data_interval_end. That's
correct for this DAG: ingest slices Kafka by [start, end) and needs a real
window, whereas retraining reads whatever fraud.gold.fct_card_velocity_features
currently holds and selects no interval at all. A window it would ignore would
be a lie in the UI.

No automated champion promotion. The notebook's PROMOTION_FLOOR promotes on
first registration only; once a champion exists, new versions register and
@champion stays put until a human moves it in the MLflow UI. Regression-gated
automatic promotion - comparing holdout metrics against the incumbent - is a
Week 4 refinement, and pretending otherwise would put an unreviewed model in
front of FastAPI.

Requires DATABRICKS_TRAINING_JOB_ID (see .env.example); create the job once
via Workflows -> Create job -> Notebook task in the Databricks UI.
"""

from __future__ import annotations

import json
import os
from datetime import timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException

# Serverless cold start plus training; well under this, but bounded so a
# wedged run surfaces as a failure instead of holding the slot indefinitely.
JOB_TIMEOUT = timedelta(hours=1)


@dag(
    dag_id="retrain_model",
    schedule="0 2 * * 0",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0},
    tags=["ml", "retrain", "mlflow"],
    doc_md=__doc__,
)
def retrain_model():

    # databricks-sdk rather than the Databricks provider: it's already pinned
    # for upload_to_volume, it reads DATABRICKS_HOST/TOKEN straight from env
    # so no Airflow Connection has to be provisioned, and compose runs no
    # triggerer service - a deferrable provider operator would hang forever.
    # retries=0 deliberately: run_now(...).result() blocks through the whole
    # training run, so a retry doesn't re-submit a lost request - it trains a
    # second model and registers a second version. On Free Edition's fair-use
    # quota that doubles the cost of every failure. Weekly cadence means a
    # failed run can just wait for a human.
    @task(retries=0)
    def trigger_training_job() -> dict:
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()
        run = w.jobs.run_now(
            job_id=int(os.environ["DATABRICKS_TRAINING_JOB_ID"])
        ).result(timeout=JOB_TIMEOUT)

        # get_run_output takes the *task* run id, not the job run id - passing
        # the latter returns an error about a multi-task run.
        output = w.jobs.get_run_output(run.tasks[0].run_id)
        raw = output.notebook_output.result if output.notebook_output else None
        if not raw:
            # Training succeeded but the workspace notebook predates the
            # dbutils.notebook.exit contract. AirflowFailException rather than
            # a retry: re-running trains a second model without fixing the
            # notebook, burning Free Edition quota to fail the same way.
            raise AirflowFailException(
                f"Job run {run.run_page_url} succeeded but returned no notebook output. "
                "The workspace copy of train_isolation_forest.py is missing its final "
                "dbutils.notebook.exit(...) cell - re-import the notebook from "
                "notebooks/train_isolation_forest.py and re-run."
            )
        payload = json.loads(raw)
        payload["run_page_url"] = run.run_page_url
        return payload

    @task
    def log_completion(payload: dict) -> None:
        host = os.environ["DATABRICKS_HOST"].removeprefix("https://").rstrip("/")
        mlflow_url = (
            f"https://{host}/ml/experiments/{payload['experiment_id']}"
            f"/runs/{payload['mlflow_run_id']}"
        )
        print(f"MLflow run:     {mlflow_url}")
        print(f"Databricks job: {payload['run_page_url']}")
        print(f"Registered:     fraud.gold.isolation_forest v{payload['model_version']}")
        print(f"Holdout metrics: {json.dumps(payload['metrics'], indent=2)}")
        print(
            "@champion unchanged unless this was the first registration - "
            "promote manually in the MLflow UI after reviewing the metrics above."
        )

    log_completion(trigger_training_job())


retrain_model()
