# Databricks notebook source
# MAGIC %md
# MAGIC # Isolation Forest fraud scorer
# MAGIC
# MAGIC Trains on `fraud.gold.fct_card_velocity_features`. Unsupervised - `.fit()`
# MAGIC never sees `is_fraud`/`fraud_type`; labels are used only for holdout eval
# MAGIC and `@champion` promotion. Runs on Databricks; `spark`/`dbutils` and the
# MAGIC MLflow tracking URI come from the notebook runtime (no `.env` needed here,
# MAGIC unlike `quality/ge_checkpoint.py`, which runs locally).

# COMMAND ----------

# MAGIC %pip install scikit-learn==1.9.0 mlflow==3.15.1 pandas==2.2.3

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import numpy as np
import pandas as pd
import mlflow
import mlflow.sklearn
from mlflow import MlflowClient
from mlflow.exceptions import MlflowException
from mlflow.models import infer_signature
from sklearn.ensemble import IsolationForest
from sklearn.metrics import confusion_matrix
from sklearn.model_selection import train_test_split

mlflow.set_registry_uri("databricks-uc")

GOLD_TABLE = "fraud.gold.fct_card_velocity_features"
MODEL_NAME = "fraud.gold.isolation_forest"
SCORED_SAMPLE_TABLE = "fraud.gold.isolation_forest_scored_sample"

# Model's input contract, in order - the Week 4 FastAPI scorer must match
# this exactly or it's training-serving skew. is_fraud/fraud_type are
# labels, never inputs.
FEATURES = [
    "amount",
    "txn_count_1h",
    "txn_count_24h",
    "amount_sum_24h",
    "amount_avg_24h",
    "amount_vs_avg_24h_ratio",
    "minutes_since_last_txn",
    "distinct_countries_24h",
    "implied_speed_kmh",
    "is_card_present",
    "is_online",
    "is_high_risk_mcc",
    "was_declined",
    "has_history",
]

FRAUD_TYPES = ["velocity", "geo_impossible", "amount_anomaly"]

# Below the table's actual ~4.6% fraud rate on purpose - flagging the true
# rate would swamp review capacity. The threshold sweep below is how you'd
# retune this against a real reviewer-capacity number.
CONTAMINATION = 0.02
RANDOM_STATE = 42
N_ESTIMATORS = 200

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load features and split
# MAGIC
# MAGIC Small enough to pull to the driver with `toPandas()`. Stratified on
# MAGIC `is_fraud` so the holdout eval stays representative (the fit itself never
# MAGIC uses the label).

# COMMAND ----------

raw_df = spark.table(GOLD_TABLE).toPandas()

train_df, holdout_df = train_test_split(
    raw_df,
    test_size=0.25,
    random_state=RANDOM_STATE,
    stratify=raw_df["is_fraud"],
)

print(f"train: {len(train_df)} rows, {train_df['is_fraud'].mean():.4f} fraud rate")
print(f"holdout: {len(holdout_df)} rows, {holdout_df['is_fraud'].mean():.4f} fraud rate")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imputation
# MAGIC
# MAGIC Every null traces to a card's first transaction (`has_history = false`) or,
# MAGIC for `implied_speed_kmh`, a CNP row / no prior card-present event.
# MAGIC
# MAGIC - `amount_avg_24h`, `minutes_since_last_txn`: train-split **median**, not a
# MAGIC   sentinel - an extreme value would make "new card" itself the anomaly
# MAGIC   signal, when `has_history` already covers that honestly.
# MAGIC - `amount_vs_avg_24h_ratio`: `1.0` (neutral - "same as itself").
# MAGIC - `implied_speed_kmh`: `0.0`, not median - null means "no signal," and the
# MAGIC   median would fabricate a plausible speed for rows that can't have one.
# MAGIC - `txn_count_1h/24h`, `amount_sum_24h`, `distinct_countries_24h`: never
# MAGIC   null, no imputation needed.
# MAGIC
# MAGIC Medians computed on the train split only, reused for holdout (no leakage).

# COMMAND ----------

MEDIAN_IMPUTE_COLUMNS = ["amount_avg_24h", "minutes_since_last_txn"]
NEUTRAL_RATIO_VALUE = 1.0
ABSENT_SIGNAL_VALUE = 0.0
BOOLEAN_FEATURES = ["is_card_present", "is_online", "is_high_risk_mcc", "was_declined", "has_history"]

train_medians = {col: train_df[col].median() for col in MEDIAN_IMPUTE_COLUMNS}
print("train-split imputation medians:", train_medians)


def prepare_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in MEDIAN_IMPUTE_COLUMNS:
        df[col] = df[col].fillna(train_medians[col])
    df["amount_vs_avg_24h_ratio"] = df["amount_vs_avg_24h_ratio"].fillna(NEUTRAL_RATIO_VALUE)
    df["implied_speed_kmh"] = df["implied_speed_kmh"].fillna(ABSENT_SIGNAL_VALUE)
    for col in BOOLEAN_FEATURES:
        df[col] = df[col].astype(int)
    return df[FEATURES]


# No scaling needed - IsolationForest splits per-feature, scale-invariant.
X_train = prepare_features(train_df)
X_holdout = prepare_features(holdout_df)

assert not X_train.isna().any().any(), "unimputed nulls reached the model"
assert not X_holdout.isna().any().any(), "unimputed nulls reached the model"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fit and score the holdout

# COMMAND ----------

model = IsolationForest(
    n_estimators=N_ESTIMATORS,
    contamination=CONTAMINATION,
    random_state=RANDOM_STATE,
    n_jobs=-1,
)
model.fit(X_train)

# score_samples: lower = more abnormal; flip sign so higher = more anomalous.
holdout_anomaly_score = -model.score_samples(X_holdout)
holdout_pred_flag = model.predict(X_holdout) == -1  # contamination-implied cutoff

y_holdout = holdout_df["is_fraud"].astype(bool).to_numpy()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Per-fraud-type recall, at the contamination-implied threshold
# MAGIC
# MAGIC Expect velocity/geo_impossible recall to be high (genuine multivariate
# MAGIC outliers) and amount_anomaly decent but lower. Overall precision at 2%
# MAGIC contamination will look modest - the threshold sweep below is what
# MAGIC contextualizes that.

# COMMAND ----------

fraud_type_rows = []
for ftype in FRAUD_TYPES:
    mask = (holdout_df["fraud_type"] == ftype).to_numpy()
    n_actual = int(mask.sum())
    recall = float(holdout_pred_flag[mask].mean()) if n_actual else float("nan")
    fraud_type_rows.append({"fraud_type": ftype, "n_actual": n_actual, "recall": recall})

fraud_type_recall_df = pd.DataFrame(fraud_type_rows)

tn, fp, fn, tp = confusion_matrix(y_holdout, holdout_pred_flag).ravel()
overall_precision = tp / (tp + fp) if (tp + fp) else float("nan")
overall_recall = tp / (tp + fn) if (tp + fn) else float("nan")
overall_fpr = fp / (fp + tn) if (fp + tn) else float("nan")

print(fraud_type_recall_df.to_string(index=False))
print(f"\noverall precision={overall_precision:.4f} recall={overall_recall:.4f} fpr={overall_fpr:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Threshold sweep
# MAGIC
# MAGIC Same holdout scores, swept across "flag top X%" cutoffs instead of the
# MAGIC single contamination-implied one - turns the flat precision above into a
# MAGIC precision/recall/FPR trade-off curve for sizing against review capacity.

# COMMAND ----------

SWEEP_FLAGGED_PCTS = [0.005, 0.01, 0.02, 0.05, 0.10, 0.15, 0.20]

sweep_rows = []
for flagged_pct in SWEEP_FLAGGED_PCTS:
    cutoff = np.quantile(holdout_anomaly_score, 1 - flagged_pct)
    pred = holdout_anomaly_score >= cutoff
    tn, fp, fn, tp = confusion_matrix(y_holdout, pred).ravel()
    sweep_rows.append(
        {
            "flagged_pct": flagged_pct,
            "score_threshold": cutoff,
            "precision": tp / (tp + fp) if (tp + fp) else float("nan"),
            "recall": tp / (tp + fn) if (tp + fn) else float("nan"),
            "fpr": fp / (fp + tn) if (fp + tn) else float("nan"),
        }
    )

threshold_sweep_df = pd.DataFrame(sweep_rows)
print(threshold_sweep_df.to_string(index=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## MLflow tracking + Unity Catalog registration
# MAGIC
# MAGIC Registers to `fraud.gold.isolation_forest`. `@champion` promotion here is
# MAGIC a sanity floor only, for a first registration with no existing champion.
# MAGIC Once a champion exists, this notebook registers a new version but leaves
# MAGIC the alias alone - regression-gated promotion against the current champion
# MAGIC is the Week 3 retrain DAG's job.

# COMMAND ----------

PROMOTION_FLOOR = {
    "overall_precision": 0.05,
    "recall_velocity": 0.50,
    "recall_geo_impossible": 0.50,
}


def meets_promotion_floor(metrics: dict) -> bool:
    return (
        metrics["overall_precision"] >= PROMOTION_FLOOR["overall_precision"]
        and metrics["recall_velocity"] >= PROMOTION_FLOOR["recall_velocity"]
        and metrics["recall_geo_impossible"] >= PROMOTION_FLOOR["recall_geo_impossible"]
    )


recall_by_type = dict(zip(fraud_type_recall_df["fraud_type"], fraud_type_recall_df["recall"]))
run_metrics = {
    "overall_precision": overall_precision,
    "overall_recall": overall_recall,
    "overall_fpr": overall_fpr,
    "recall_velocity": recall_by_type["velocity"],
    "recall_geo_impossible": recall_by_type["geo_impossible"],
    "recall_amount_anomaly": recall_by_type["amount_anomaly"],
}

with mlflow.start_run(run_name="isolation_forest_train") as run:
    mlflow.log_params(
        {
            "contamination": CONTAMINATION,
            "n_estimators": N_ESTIMATORS,
            "random_state": RANDOM_STATE,
            "train_rows": len(X_train),
            "holdout_rows": len(X_holdout),
            "features": ",".join(FEATURES),
        }
    )
    mlflow.log_metrics(run_metrics)
    mlflow.log_table(threshold_sweep_df, artifact_file="threshold_sweep.json")

    signature = infer_signature(X_train, model.predict(X_train))
    mlflow.sklearn.log_model(
        sk_model=model,
        name="model",
        signature=signature,
        input_example=X_train.head(5),
    )
    model_uri = f"runs:/{run.info.run_id}/model"

client = MlflowClient()
registered_version = mlflow.register_model(model_uri=model_uri, name=MODEL_NAME)

try:
    client.get_model_version_by_alias(MODEL_NAME, "champion")
    has_champion = True
except MlflowException:
    has_champion = False

if not has_champion and meets_promotion_floor(run_metrics):
    client.set_registered_model_alias(MODEL_NAME, "champion", registered_version.version)
    print(f"Promoted {MODEL_NAME} v{registered_version.version} to @champion (first registration, floor met).")
elif has_champion:
    print(
        f"Registered {MODEL_NAME} v{registered_version.version}; left @champion untouched "
        "(regression-gated promotion over an existing champion happens in the Week 3 retrain DAG)."
    )
else:
    print(
        f"Registered {MODEL_NAME} v{registered_version.version}; did not promote — "
        f"holdout metrics below the promotion floor {PROMOTION_FLOOR}."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scored sample table
# MAGIC
# MAGIC Every holdout fraud plus a random non-fraud sample, so flagged/missed
# MAGIC transactions can be spot-checked from Databricks SQL or Streamlit without
# MAGIC standing up FastAPI first. Notebook-managed, not a dbt model - a
# MAGIC `dbt run --full-refresh` on gold won't touch it.

# COMMAND ----------

SAMPLE_NONFRAUD_N = 1000

scored_holdout_df = holdout_df[
    ["transaction_id", "card_id_hash", "event_time", "amount", "is_fraud", "fraud_type"]
].copy()
scored_holdout_df["anomaly_score"] = holdout_anomaly_score
scored_holdout_df["predicted_fraud"] = holdout_pred_flag
scored_holdout_df["model_name"] = MODEL_NAME
scored_holdout_df["model_version"] = registered_version.version
scored_holdout_df["mlflow_run_id"] = run.info.run_id

fraud_rows = scored_holdout_df[scored_holdout_df["is_fraud"].astype(bool)]
nonfraud_rows = scored_holdout_df[~scored_holdout_df["is_fraud"].astype(bool)].sample(
    n=min(SAMPLE_NONFRAUD_N, (~scored_holdout_df["is_fraud"].astype(bool)).sum()),
    random_state=RANDOM_STATE,
)
scored_sample_df = pd.concat([fraud_rows, nonfraud_rows], ignore_index=True)

spark.createDataFrame(scored_sample_df).write.mode("overwrite").saveAsTable(SCORED_SAMPLE_TABLE)
print(f"wrote {len(scored_sample_df)} rows ({len(fraud_rows)} fraud, {len(nonfraud_rows)} non-fraud) to {SCORED_SAMPLE_TABLE}")
