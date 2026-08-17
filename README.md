# Real-Time Fraud Detection Pipeline

## Project overview

A pipeline that simulates how banks detect fraudulent card transactions in real time. It generates realistic card transactions with their full payment lifecycle: each purchase is authorized immediately, then typically settles for payment hours or days later. It pushes them through the same kind of system a payments company would run: a streaming ingestion layer, a data lakehouse, automated data-quality checks, and a machine learning model that flags suspicious activity. Built on Kafka, Airflow, Databricks (Unity Catalog, Delta Lake), dbt, Great Expectations, an Isolation Forest model tracked with MLflow, and a FastAPI + Streamlit front end.

## Features

- Simulates realistic card transaction traffic, authorizations and their later settlements, with three injected fraud patterns (rapid-fire bursts, geographically impossible transaction pairs, abnormal amounts), delivered through a Kafka stream.
- Moves that data from the stream into a governed lakehouse on a fixed schedule, using three Airflow pipelines: ingestion, transformation, and a weekly model retrain.
- Organizes raw data into clean, analysis-ready tables through progressive layers (raw → cleaned → business-ready), built with Databricks and dbt, including automated tests on every layer.
- Matches every authorization to its eventual settlement, and flags it when one never arrives, so payment discrepancies surface automatically instead of getting buried.
- Keeps a full history of merchant details as they change over time, not just their current snapshot, using a dbt snapshot.
- Blocks bad data from reaching the machine learning model with an automated quality gate that halts the pipeline if key metrics fall outside expected ranges.
- Flags fraudulent transactions using two complementary methods at once, fast deterministic rules plus a machine learning model, with every model version tracked and promoted through MLflow.
- Serves live fraud scores over an API that stays available (in a visibly degraded state) even if the model registry is temporarily unreachable, built with FastAPI.
- Displays pipeline health and fraud-detection results on a live dashboard, built with Streamlit.

## Tech stack

- **Streaming:** Kafka 3.9 (KRaft, no ZooKeeper), confluent-kafka 2.6.1
- **Orchestration:** Airflow 3.3.1, TaskFlow API, asset-based cross-DAG scheduling
- **Lakehouse:** Databricks Free Edition, Delta Lake, Unity Catalog (catalog/schema/volume)
- **Transformation:** dbt-databricks 1.9.4 / dbt-core 1.12.2 (medallion models, snapshot, tests)
- **Quality:** Great Expectations 1.20.0 (batch checkpoint gate)
- **ML/MLOps:** scikit-learn 1.9.0 (Isolation Forest), MLflow 3.15.1 (tracking + UC model registry)
- **Serving:** FastAPI 0.115.6, Pydantic 2.10.4, Streamlit 1.41.1
- **Infrastructure:** Docker Compose, Postgres 16 (Airflow metadata), Databricks SDK / SQL connector

## Key design decisions

**Three-layer ingest idempotency.** `consume_batch` bounds each Kafka read to `[data_interval_start, data_interval_end)` via `offsets_for_times`, so a retried interval always resolves the same offset range; `upload_to_volume` writes a deterministic filename with `overwrite=True`, so a retry replaces rather than duplicates; `COPY INTO` tracks loaded file paths itself. Re-running the same interval converges to the same Bronze state instead of duplicate rows.

**GE gate protecting the non-idempotent snapshot.** `transform_quality` runs `dbt build` (models + tests, snapshot excluded), then a Great Expectations checkpoint against Gold, then `dbt snapshot` last, gated on the checkpoint's exit code. Every other model here is an idempotent rebuild; the merchant SCD2 snapshot is an irreversible append, so it's the one step held behind quality gating.

**Hybrid rules + model scoring.** Three deterministic threshold rules (velocity, geo, amount) run alongside the Isolation Forest and are OR-ed into one verdict: the rules catch most fraud outright and cheaply, while the model catches multivariate cases that fall under a rule's threshold. See [Model performance](#model-performance) for the recall breakdown this produces.

## Setup and run

**Prerequisites:** Docker Desktop, Python 3.11+ (for local producer/tests), a Databricks Free Edition workspace.

**1. Environment**
```
cp .env.example .env
# fill in DATABRICKS_HOST, DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN, DATABRICKS_TRAINING_JOB_ID
```

**2. Databricks workspace setup**
- Create catalog `fraud` with schemas `bronze`, `silver`, `gold`
- Create UC Volume `fraud.bronze.landing` (the Kafka → Bronze bridge's landing zone)
- Create a serverless SQL warehouse; copy its HTTP path into `DATABRICKS_HTTP_PATH`
- Generate a personal access token for `DATABRICKS_TOKEN`
- Import `notebooks/train_isolation_forest.py` into the workspace, create a Job (Workflows → Create job → Notebook task) pointing at it, and copy the job ID into `DATABRICKS_TRAINING_JOB_ID`

**3. Bring up the local stack**
```
docker compose up -d
```

**4. Generate initial data**
```
pip install -r requirements.txt
python producer.py --time-compression 60 --count 5000
```
Then unpause `ingest_transactions` in the Airflow UI (runs every 15 min, or trigger manually). Its success feeds `transform_quality` via an asset dependency.

**5. Access points**
| Service | URL |
|---|---|
| Kafka UI | http://localhost:8080 |
| Airflow UI | http://localhost:8081 (admin/admin) |
| FastAPI | http://localhost:8082 (`/docs` for OpenAPI) |
| Streamlit dashboard | http://localhost:8083 |

## Model performance

Holdout evaluation, Isolation Forest vs. the three deterministic rules vs. both combined:

**Recall by fraud type**

| fraud_type | Rules only | Model only | Hybrid |
|---|---|---|---|
| velocity | 0.623 | 0.255 | 0.642 |
| geo_impossible | 0.533 | 0.111 | 0.533 |
| amount_anomaly | 0.737 | 0.684 | 0.737 |

**Overall**

| | Precision | Recall | FPR |
|---|---|---|---|
| Rules only | 0.867 | 0.612 | 0.005 |
| Model only | 0.495 | 0.265 | 0.013 |
| Hybrid | 0.646 | 0.624 | 0.016 |

Hybrid recall matches rules-only on geo_impossible and amount_anomaly (the model adds nothing there) but beats it on velocity (0.642 vs. 0.623). The model catches burst rows before `txn_count_1h` crosses the rule's threshold, which is the entire source of hybrid's overall recall gain (0.612 to 0.624), paid for with a precision drop (0.867 to 0.646) from the model's added false positives.


