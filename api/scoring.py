"""Serving-side copy of the model's input contract.

Single source of truth for the API. FEATURES and RULES are duplicated from
notebooks/train_isolation_forest.py rather than imported because a Databricks
notebook can't import from this repo - api/tests/test_scoring.py asserts the
two copies stay identical, which is the only thing standing between a silent
edit on one side and training-serving skew.

Deliberately free of FastAPI imports so the rule logic is testable without an
app or a network.
"""

from __future__ import annotations

import pandas as pd

MODEL_NAME = "fraud.gold.isolation_forest"
CHAMPION_ALIAS = "champion"
MODEL_URI = f"models:/{MODEL_NAME}@{CHAMPION_ALIAS}"

# Order is the model's positional input contract, not decoration - a reordering
# here scores every request against the wrong columns without erroring.
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

BOOLEAN_FEATURES = ["is_card_present", "is_online", "is_high_risk_mcc", "was_declined", "has_history"]

# Thresholds mirror the notebook exactly, including the shape of each entry, so
# the parity test compares structures rather than a re-spelled version of them.
RULES = {
    "VELOCITY_RULE": {"column": "txn_count_1h", "threshold": 2},
    "GEO_RULE": {"column": "implied_speed_kmh", "threshold": 1500},
    "AMOUNT_RULE": {"column": "amount_vs_avg_24h_ratio", "threshold": 15},
}

# Notebook key names on the left (parity), API surface on the right.
RULE_RESPONSE_NAMES = {
    "VELOCITY_RULE": "velocity",
    "GEO_RULE": "geo",
    "AMOUNT_RULE": "amount",
}


def apply_rules(values: dict) -> dict[str, bool]:
    """One boolean per rule, keyed by API name. Mirrors the notebook's
    evaluate_rules: strict `>`, evaluated on raw request values rather than
    the float-cast frame the model sees."""
    return {
        RULE_RESPONSE_NAMES[name]: bool(values[spec["column"]] > spec["threshold"])
        for name, spec in RULES.items()
    }


def build_feature_frame(values: dict) -> pd.DataFrame:
    """Single-row frame in FEATURES order. astype(float) mirrors the notebook's
    prepare_features, so request bools reach the model as 1.0/0.0 exactly as
    they did in training."""
    return pd.DataFrame([[values[c] for c in FEATURES]], columns=FEATURES).astype(float)
