# Real-Time Fraud Detection Pipeline

## Project overview

This pipeline simulates how banks catch fraudulent card transactions in real time. It generates realistic transactions with a full payment lifecycle. Each purchase is authorized right away, then usually settles for payment hours or days later. That data flows through the kind of system a real payments company would run. There's a streaming layer, a data lakehouse, automated quality checks, and a machine learning model that flags anything suspicious. It's built on Kafka, Airflow, Databricks (Unity Catalog, Delta Lake), dbt, Great Expectations, an Isolation Forest model tracked with MLflow, and a FastAPI plus Streamlit front end.

## Features

- Simulates realistic card transaction traffic through Kafka. That includes authorizations, their later settlements, and three injected fraud patterns (rapid bursts, geographically impossible transaction pairs, abnormal amounts).
- Moves that data into a governed lakehouse on a schedule, using three Airflow pipelines for ingestion, transformation, and a weekly model retrain.
- Organizes raw data into clean, analysis-ready tables through progressive layers (raw, cleaned, business-ready), built with Databricks and dbt. Every layer has automated tests.
- Matches every authorization to its eventual settlement and flags the ones that never get one. Payment discrepancies surface automatically instead of getting buried.
- Keeps a running history of merchant details with a dbt snapshot, so past values stay queryable even after a merchant's name or category changes.
- Blocks bad data from reaching the model with an automated quality gate. It halts the pipeline if key metrics fall outside expected ranges.
- Flags fraud with two methods running together, fast deterministic rules and a machine learning model. Every model version gets tracked and promoted through MLflow.
- Serves live fraud scores over a FastAPI endpoint. If the model registry goes down, the API stays up and reports itself as degraded instead of crashing.
- Displays pipeline health and fraud-detection results on a live Streamlit dashboard.

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

**Three-layer ingest idempotency.** Three things make re-running the same interval safe. `consume_batch` bounds each Kafka read to `[data_interval_start, data_interval_end)` via `offsets_for_times`, so a retry always resolves the same offset range. `upload_to_volume` writes a deterministic filename with `overwrite=True`, so a retry replaces the file instead of duplicating it. `COPY INTO` tracks loaded file paths on its own. Re-running the same interval converges to the same Bronze state instead of piling up duplicate rows.

**GE gate protecting the non-idempotent snapshot.** `transform_quality` runs `dbt build` first (models and tests, snapshot excluded), then a Great Expectations checkpoint against Gold, then `dbt snapshot` last. The snapshot only runs if the checkpoint passes. Every other model here is an idempotent rebuild. The merchant SCD2 snapshot is not. It's an irreversible append, so it's the one step held behind quality gating.

**Hybrid rules + model scoring.** Three deterministic threshold rules (velocity, geo, amount) run alongside the Isolation Forest, OR-ed into one verdict. The rules catch most fraud outright, cheaply. The model picks up multivariate cases that fall under a rule's threshold. See [Model performance](#model-performance) for the recall breakdown this produces.

## Setup and run

**Prerequisites:** Docker Desktop, Python 3.11+ (for local producer/tests), a Databricks Free Edition workspace.

**1. Environment**
```
cp .env.example .env
# fill in DATABRICKS_HOST, DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN, DATABRICKS_TRAINING_JOB_ID
```

**2. Databricks workspace setup**
- Create catalog `fraud` with schemas `bronze`, `silver`, `gold`
- Create UC Volume `fraud.bronze.landing` (the Kafka to Bronze bridge's landing zone)
- Create a serverless SQL warehouse and copy its HTTP path into `DATABRICKS_HTTP_PATH`
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

Numbers below come from holdout evaluation. They compare the Isolation Forest alone, the three rules alone, and both together.

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

Hybrid recall matches rules-only on geo_impossible and amount_anomaly. The model doesn't add anything there. On velocity it does better, 0.642 versus 0.623 for rules alone. That's because the model catches burst rows before `txn_count_1h` crosses the rule's threshold. It's the entire source of hybrid's overall recall gain, from 0.612 up to 0.624. The cost is precision, which drops from 0.867 to 0.646 as the model adds its own false positives.

## Known limitations and future work

- Imputation medians (`amount_avg_24h`, `minutes_since_last_txn`) get recomputed on every training run. They're never logged as MLflow params or artifacts, so you can't reproduce a served `@champion`'s exact imputation values from the registry alone.
- `recon_amount_tolerance_pct` is one flat rate (10%) across all MCCs. Real card networks vary drift tolerance by MCC. Restaurants and hospitality tolerate more tip-driven drift than retail does.
- Promotion beyond first registration is manual. A human moves `@champion` in the MLflow UI. There's no regression-gated automatic promotion against the incumbent's holdout metrics.
- FastAPI scores each request against Gold-computed velocity features, with no live feature cache. A real-time deployment would need an online feature store instead of point-in-time reads off a batch table.
- Settlement-drift or MCC-tolerance changes need a full producer data reset. History generated before the three-way drift mixture was introduced isn't retroactively consistent with it.
