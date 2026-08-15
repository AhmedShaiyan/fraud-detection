"""Request/response models for the scoring API."""

from __future__ import annotations

from pydantic import BaseModel, Field


class TransactionFeatures(BaseModel):
    """The 14 model features plus card_id_hash.

    Every field is required. Training imputed amount_avg_24h and
    minutes_since_last_txn with train-split medians that were never logged to
    MLflow, so the service can't reproduce that imputation - requiring
    non-null pushes it to the caller instead of silently inventing a value.
    """

    # Logging/traceability only - never reaches the model.
    card_id_hash: str

    amount: float
    txn_count_1h: float
    txn_count_24h: float
    amount_sum_24h: float
    amount_avg_24h: float
    amount_vs_avg_24h_ratio: float
    minutes_since_last_txn: float
    distinct_countries_24h: float
    implied_speed_kmh: float

    # Cast to 1.0/0.0 before the model sees them, matching training.
    is_card_present: bool
    is_online: bool
    is_high_risk_mcc: bool
    was_declined: bool
    has_history: bool


class RuleFlags(BaseModel):
    velocity: bool
    geo: bool
    amount: bool


class ScoreResponse(BaseModel):
    # Higher = more anomalous (the notebook's -score_samples convention).
    anomaly_score: float
    model_flag: bool
    rule_flags: RuleFlags
    combined_flag: bool
    scored_at: str


class HealthResponse(BaseModel):
    status: str
    model_version: str | None
    champion_alias: str
    # Populated only when status is "degraded".
    detail: str | None = None


class ModelResponse(BaseModel):
    model_name: str
    model_version: str
    champion_alias: str
    run_id: str | None
    features: list[str]
    loaded_at: str

    # model_* field names collide with pydantic v2's protected namespace.
    model_config = {"protected_namespaces": ()}
