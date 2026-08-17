"""Weekly Isolation Forest retrain on Databricks, Sundays 02:00 UTC.

Triggers the training notebook as a Databricks job (needs spark/dbutils, so
it can only run there) and reports where the resulting MLflow run landed.

Bare cron, not CronDataIntervalTimetable like ingest_transactions: training
reads whatever fraud.gold.fct_card_velocity_features currently holds, no
interval to slice.

No automated champion promotion: PROMOTION_FLOOR only promotes on first
registration; after that @champion moves only via a human in the MLflow UI.

Requires DATABRICKS_TRAINING_JOB_ID (see .env.example); create the job once
via Workflows -> Create job -> Notebook task in the Databricks UI.
"""

from __future__ import annotations

import json
import os
from datetime import timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException

# generous bound so a wedged run fails instead of holding the slot forever
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

    # databricks-sdk over the Databricks provider: already pinned for
    # upload_to_volume, reads DATABRICKS_HOST/TOKEN from env directly, and
    # compose runs no triggerer service that a deferrable operator would need.
    # retries=0: run_now(...).result() blocks through the whole run, so a
    # retry trains and registers a second model rather than resuming - costly
    # on Free Edition's quota. Weekly cadence means a failure can wait for a human.
    @task(retries=0)
    def trigger_training_job() -> dict:
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()
        run = w.jobs.run_now(
            job_id=int(os.environ["DATABRICKS_TRAINING_JOB_ID"])
        ).result(timeout=JOB_TIMEOUT)

        # get_run_output wants the *task* run id, not the job run id
        output = w.jobs.get_run_output(run.tasks[0].run_id)
        raw = output.notebook_output.result if output.notebook_output else None
        if not raw:
            # not a retry: re-running just trains another model and fails the same way
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
