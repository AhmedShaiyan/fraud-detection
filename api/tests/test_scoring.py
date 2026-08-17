"""Rule logic and the training-serving parity guard.

No app, no network, no Databricks - everything here is pure functions and
source text.
"""

import ast
import sys
from pathlib import Path

import pytest

API_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(API_DIR))

import scoring  # noqa: E402

NOTEBOOK = API_DIR.parent / "notebooks" / "train_isolation_forest.py"


def _notebook_literal(name: str):
    """Read a module-level constant out of the notebook without running it.

    The notebook can't be imported: it calls dbutils.library.restartPython()
    and spark.table() at module level, so an import raises NameError outside
    Databricks. Parsing gets the same literal with nothing executed.
    """
    tree = ast.parse(NOTEBOOK.read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == name for t in node.targets
        ):
            return ast.literal_eval(node.value)
    raise AssertionError(f"{name} not found at module level in {NOTEBOOK}")


# training-serving parity


def test_features_match_notebook():
    # Order-sensitive on purpose: FEATURES is the model's positional input
    # contract, so a reordering is silent skew rather than a visible error.
    assert _notebook_literal("FEATURES") == scoring.FEATURES


def test_rules_match_notebook():
    # Catches a threshold retuned on one side only.
    assert _notebook_literal("RULES") == scoring.RULES


def test_boolean_features_match_notebook():
    assert _notebook_literal("BOOLEAN_FEATURES") == scoring.BOOLEAN_FEATURES


def test_every_rule_column_is_a_feature():
    for spec in scoring.RULES.values():
        assert spec["column"] in scoring.FEATURES


def test_every_rule_has_a_response_name():
    assert set(scoring.RULE_RESPONSE_NAMES) == set(scoring.RULES)


# rule boundaries

CLEAN = {
    "amount": 10.0,
    "txn_count_1h": 0.0,
    "txn_count_24h": 1.0,
    "amount_sum_24h": 10.0,
    "amount_avg_24h": 10.0,
    "amount_vs_avg_24h_ratio": 1.0,
    "minutes_since_last_txn": 120.0,
    "distinct_countries_24h": 1.0,
    "implied_speed_kmh": 0.0,
    "is_card_present": True,
    "is_online": False,
    "is_high_risk_mcc": False,
    "was_declined": False,
    "has_history": True,
}


def _flags(**overrides):
    return scoring.apply_rules({**CLEAN, **overrides})


def test_clean_row_fires_nothing():
    assert _flags() == {"velocity": False, "geo": False, "amount": False}


# Thresholds are strict `>`, so the threshold value itself must not fire.
@pytest.mark.parametrize(
    "field, rule, at_threshold, above",
    [
        ("txn_count_1h", "velocity", 2, 3),
        ("implied_speed_kmh", "geo", 1500, 1500.01),
        ("amount_vs_avg_24h_ratio", "amount", 15, 15.01),
    ],
)
def test_rule_boundary(field, rule, at_threshold, above):
    assert _flags(**{field: at_threshold})[rule] is False
    assert _flags(**{field: above})[rule] is True


def test_rules_are_independent():
    # One rule firing must not drag the others with it.
    assert _flags(implied_speed_kmh=9000) == {"velocity": False, "geo": True, "amount": False}


# frame construction


def test_frame_column_order_matches_features():
    assert list(scoring.build_feature_frame(CLEAN).columns) == scoring.FEATURES


def test_frame_is_all_float_including_booleans():
    frame = scoring.build_feature_frame(CLEAN)
    assert all(str(d) == "float64" for d in frame.dtypes)
    # bool True must reach the model as 1.0, exactly as in training.
    assert frame["is_card_present"].iloc[0] == 1.0
    assert frame["is_online"].iloc[0] == 0.0


def test_frame_ignores_extra_keys():
    # card_id_hash rides along in the request but is never a feature.
    frame = scoring.build_feature_frame({**CLEAN, "card_id_hash": "abc123"})
    assert "card_id_hash" not in frame.columns


# combined_flag composition


def test_combined_flag_fires_on_rule_alone():
    # Mirrors main.py: model_flag OR any rule. A row the model thinks is clean
    # is still flagged when a rule trips.
    rule_flags = _flags(txn_count_1h=9)
    assert (False or any(rule_flags.values())) is True


def test_combined_flag_false_when_nothing_fires():
    assert (False or any(_flags().values())) is False
