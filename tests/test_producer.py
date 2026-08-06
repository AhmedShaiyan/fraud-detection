import json
import random
import sys
from pathlib import Path

import pyarrow.parquet as pq
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import producer as p


@pytest.fixture(autouse=True)
def _reseed():
    random.seed(42)
    p.Faker.seed(42)


@pytest.fixture
def cards():
    return p.build_card_pool(n_cards=20)


@pytest.fixture
def merchants():
    return p.build_merchant_pool(n_merchants=20)


def test_settlement_references_real_auth(cards, merchants):
    card = cards[0]
    settlement = None
    auth = None
    for _ in range(200):
        auth = p.generate_auth_batch(card, merchants, fraud_rate=0.0)[0]
        settlement, _ = p.decide_settlement(auth, (2, 10))
        if settlement is not None:
            break
    assert settlement is not None, "expected at least one settlement in 200 tries"
    assert settlement["auth_transaction_id"] == auth["transaction_id"]
    assert settlement["event_type"] == "SETTLEMENT"


def test_settlement_rate_approx_95_percent(cards, merchants):
    n = 3000
    settled = 0
    for _ in range(n):
        card = random.choice(cards)
        auth = p.generate_auth_batch(card, merchants, fraud_rate=0.0)[0]
        auth["auth_code"] = "00"  # isolate the 95%/5% split from the decline carve-out
        settlement, _ = p.decide_settlement(auth, (2, 10))
        if settlement is not None:
            settled += 1
    rate = settled / n
    assert abs(rate - 0.95) < 0.03, f"settlement rate {rate:.3f} not close to 0.95"


def test_fraud_rate_approx_1_5_percent(cards, merchants):
    # fraud_rate is a per-decision probability (one card draw = one decision);
    # fraud generators can burst into several events, so the *event-level*
    # fraud fraction runs well above fraud_rate by design. Measure at the
    # decision level, which is what --fraud-rate actually controls.
    n = 5000
    fraud_decisions = 0
    for _ in range(n):
        card = random.choice(cards)
        batch = p.generate_auth_batch(card, merchants, fraud_rate=0.015)
        if batch[0]["is_fraud"]:
            fraud_decisions += 1
    rate = fraud_decisions / n
    assert abs(rate - 0.015) < 0.01, f"fraud rate {rate:.4f} not close to 0.015"


def test_velocity_events_share_card_id(cards, merchants):
    card = cards[0]
    txns = p.fraud_velocity(card, merchants)
    assert len(txns) >= 5
    assert all(t["card_id"] == card["card_id"] for t in txns)
    assert all(t["fraud_type"] == "velocity" for t in txns)
    assert all(t["is_fraud"] == 1 for t in txns)


def test_declined_auth_never_settles(cards, merchants):
    n = 500
    declined_ids = set()
    settlements = []
    for _ in range(n):
        card = random.choice(cards)
        auth = p.generate_auth_batch(card, merchants, fraud_rate=0.0)[0]
        settlement, delay = p.decide_settlement(auth, (2, 10))
        if auth["auth_code"] != "00":
            declined_ids.add(auth["transaction_id"])
            assert settlement is None
            assert delay == 0
        elif settlement is not None:
            settlements.append(settlement)
    assert declined_ids, "expected at least one declined auth in 500 tries"
    assert not any(s["auth_transaction_id"] in declined_ids for s in settlements)


def test_parquet_schema_matches_kafka_json_schema(cards, merchants, tmp_path):
    events = p.generate_events(50, cards, merchants, fraud_rate=0.015, delay_range=(2, 10))
    assert len(events) == 50

    json_keys = set(json.loads(json.dumps(events[0])).keys())
    assert json_keys == set(p.EVENT_FIELDS)

    path = p.write_parquet(events, tmp_path)
    table = pq.read_table(path)
    assert table.column_names == p.EVENT_FIELDS
    assert table.num_rows == 50
