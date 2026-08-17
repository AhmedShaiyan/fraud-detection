"""FastAPI scoring service for the @champion Isolation Forest.

Serves the same hybrid verdict the training notebook computes offline: model
anomaly score OR-ed with the three deterministic rules.

Loads mlflow.sklearn, not mlflow.pyfunc: pyfunc only exposes predict()
(1/-1), and anomaly_score needs score_samples(), which only the sklearn
flavour hands back.

A registry failure at startup is recorded, not raised, so the container
comes up degraded instead of crash-looping - `docker compose up` must bring
every local service up healthy.
"""

from __future__ import annotations

import logging
import os
import threading
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException

from schemas import (
    HealthResponse,
    ModelResponse,
    RuleFlags,
    ScoreResponse,
    TransactionFeatures,
)
from scoring import (
    CHAMPION_ALIAS,
    FEATURES,
    MODEL_NAME,
    MODEL_URI,
    apply_rules,
    build_feature_frame,
)

logger = logging.getLogger("uvicorn.error")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_champion(app: FastAPI) -> None:
    import mlflow
    from mlflow import MlflowClient

    # Auth is DATABRICKS_HOST/DATABRICKS_TOKEN straight from the environment,
    # the same way every other service in this repo authenticates.
    mlflow.set_registry_uri("databricks-uc")

    version = MlflowClient().get_model_version_by_alias(MODEL_NAME, CHAMPION_ALIAS)
    model = mlflow.sklearn.load_model(MODEL_URI)

    # The estimator carries the column order it was fitted on. The parity test
    # only proves this file agrees with the notebook's source; this catches the
    # case the test can't see - a newly promoted champion trained on different
    # or reordered features. Refuse to serve rather than score against columns
    # that silently mean something else.
    trained_on = list(getattr(model, "feature_names_in_", []))
    if trained_on and trained_on != FEATURES:
        raise RuntimeError(
            f"champion v{version.version} was trained on {trained_on}, "
            f"but this service serves {FEATURES}"
        )

    app.state.model = model
    app.state.model_version = version.version
    app.state.run_id = version.run_id
    app.state.loaded_at = _utc_now()


def _load_in_background(app: FastAPI) -> None:
    if not os.environ.get("DATABRICKS_HOST") or not os.environ.get("DATABRICKS_TOKEN"):
        app.state.load_error = "DATABRICKS_HOST/DATABRICKS_TOKEN not set"
        logger.error("startup: %s", app.state.load_error)
        return
    try:
        _load_champion(app)
        logger.info("loaded %s@%s v%s", MODEL_NAME, CHAMPION_ALIAS, app.state.model_version)
    except Exception as exc:
        app.state.load_error = f"{type(exc).__name__}: {exc}"
        logger.exception("startup: could not load %s", MODEL_URI)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.model = None
    app.state.model_version = None
    app.state.run_id = None
    app.state.loaded_at = None
    app.state.load_error = None

    # Loaded off the startup path on purpose. An unreachable DATABRICKS_HOST
    # makes the SDK's host-metadata lookup block for a full 5 minutes; doing
    # that inside lifespan means uvicorn never finishes starting, so the
    # service answers nothing at all instead of answering "degraded". A daemon
    # thread lets /health respond immediately and report the real state.
    threading.Thread(target=_load_in_background, args=(app,), daemon=True).start()

    yield


app = FastAPI(title="Fraud scoring API", lifespan=lifespan)


# load_error is None while the background load is still in flight, which is a
# different situation from a load that failed - don't report them the same way.
LOADING = "model still loading"


def _unavailable_reason() -> str:
    return app.state.load_error or LOADING


def _require_model() -> None:
    if app.state.model is None:
        raise HTTPException(status_code=503, detail=f"model unavailable: {_unavailable_reason()}")


@app.post("/score", response_model=ScoreResponse)
def score(txn: TransactionFeatures) -> ScoreResponse:
    _require_model()

    values = txn.model_dump()
    frame = build_feature_frame(values)

    # Flip the sign so higher = more anomalous, matching the notebook.
    anomaly_score = float(-app.state.model.score_samples(frame)[0])
    model_flag = bool(app.state.model.predict(frame)[0] == -1)
    # Rules read the raw request values, not the float frame - they're meant to
    # run on real incoming values (same choice the notebook makes).
    rule_flags = apply_rules(values)

    return ScoreResponse(
        anomaly_score=anomaly_score,
        model_flag=model_flag,
        rule_flags=RuleFlags(**rule_flags),
        combined_flag=model_flag or any(rule_flags.values()),
        scored_at=_utc_now(),
    )


@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    # Always 200: this reports state, it doesn't gate traffic.
    loaded = app.state.model is not None
    return HealthResponse(
        status="healthy" if loaded else "degraded",
        model_version=app.state.model_version,
        champion_alias=f"@{CHAMPION_ALIAS}",
        detail=None if loaded else _unavailable_reason(),
    )


@app.get("/model", response_model=ModelResponse)
def model_metadata() -> ModelResponse:
    _require_model()
    return ModelResponse(
        model_name=MODEL_NAME,
        model_version=app.state.model_version,
        champion_alias=f"@{CHAMPION_ALIAS}",
        run_id=app.state.run_id,
        features=FEATURES,
        loaded_at=app.state.loaded_at,
    )
