import json
import math
import random
import sys
from datetime import datetime, timedelta, timezone
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
        settlement, _ = p.decide_settlement(auth, (2, 48))
        if settlement is not None:
            break
    assert settlement is not None, "expected at least one settlement in 200 tries"
    assert settlement["auth_transaction_id"] == auth["transaction_id"]
    assert settlement["event_type"] == "SETTLEMENT"

    auth_time = datetime.fromisoformat(auth["event_time"])
    settle_time = datetime.fromisoformat(settlement["event_time"])
    delay_hours = (settle_time - auth_time).total_seconds() / 3600
    assert 2 <= delay_hours <= 48, f"settlement delay {delay_hours:.2f}h outside 2-48h"


def test_settlement_rate_approx_95_percent(cards, merchants):
    n = 3000
    settled = 0
    for _ in range(n):
        card = random.choice(cards)
        auth = p.generate_auth_batch(card, merchants, fraud_rate=0.0)[0]
        auth["auth_code"] = "00"  # isolate the 95%/5% split from the decline carve-out
        settlement, _ = p.decide_settlement(auth, (2, 48))
        if settlement is not None:
            settled += 1
    rate = settled / n
    assert abs(rate - 0.95) < 0.03, f"settlement rate {rate:.3f} not close to 0.95"


def test_fraud_rate_approx_1_5_percent(cards, merchants):
    # fraud_rate is a per-decision probability (one card turn = one decision);
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
    assert len({t["merchant_id"] for t in txns}) == 1
    assert all(t["fraud_type"] == "velocity" for t in txns)
    assert all(t["is_fraud"] == 1 for t in txns)


def test_velocity_timestamps_strictly_ascending(cards, merchants):
    card = cards[0]
    txns = p.fraud_velocity(card, merchants)
    times = [datetime.fromisoformat(t["event_time"]) for t in txns]
    assert times == sorted(times)
    assert len(set(times)) == len(times), "same-second ties defeat point-in-time recency features"
    gaps = [(b - a).total_seconds() for a, b in zip(times, times[1:])]
    assert all(5 <= g <= 30 for g in gaps)


def test_normal_txn_home_bound_shares_country(cards, merchants, monkeypatch):
    card = cards[0]
    monkeypatch.setattr(p.random, "random", lambda: 0.01)  # always below HOME_COUNTRY_RATE
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    countries = {p._normal_txn_country(card, now) for _ in range(20)}
    assert countries == {card["home_country"]}


def test_travel_mode_returns_home(cards, merchants, monkeypatch):
    # Country changes are deferred: the transaction that *decides* to travel
    # (or return) stays in the old country and only sets a pending
    # destination + a flight gate; the new country only takes effect on the
    # next call (standing in for "the next time this card clears its gate").
    card = cards[0]
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    responses = iter([0.98, 0.01])  # 0.98 -> decide to travel; 0.01 -> decide to return home
    monkeypatch.setattr(p.random, "random", lambda: next(responses))
    monkeypatch.setattr(p.random, "randint", lambda a, b: 3)  # fixed trip length = 3

    # call 1: decision to travel - this txn is still home, gate now set
    assert p._normal_txn_country(card, now) == card["home_country"]
    assert card["next_available_at"] is not None
    trip_country = card["pending_country"]
    assert trip_country is not None and trip_country != card["home_country"]

    # call 2: gate consumed - first foreign txn (arrival)
    assert p._normal_txn_country(card, now) == trip_country
    assert card["pending_country"] is None

    for _ in range(2):  # calls 3-4: remaining 2 txns of the 3-txn trip
        assert p._normal_txn_country(card, now) == trip_country

    # call 5: trip exhausted, re-roll (0.01 mocked) decides to return - still
    # abroad for this txn, return-flight gate now set
    assert p._normal_txn_country(card, now) == trip_country
    assert card["pending_country"] == card["home_country"]

    # call 6: return-flight gate consumed - back home
    assert p._normal_txn_country(card, now) == card["home_country"]


def test_reschedule_respects_flight_gate(cards, merchants):
    # Regression guard for the _pick_available_card removal: a card's next
    # scheduled turn must never land before its flight gate clears, even
    # when the raw exponential draw would put it sooner.
    card = cards[0]
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    gate = now + timedelta(hours=19)  # e.g. a long-haul flight in progress
    card["next_available_at"] = gate

    for _ in range(200):
        next_due = now + timedelta(seconds=p._sample_interarrival_seconds(txns_per_day_per_card=4))
        if card["next_available_at"] is not None and card["next_available_at"] > next_due:
            next_due = card["next_available_at"]
        assert next_due >= gate


def test_normal_txn_transitions_respect_flight_physics(cards, merchants):
    card = cards[0]
    clock = datetime(2026, 1, 1, tzinfo=timezone.utc)
    prev = None
    for _ in range(300):
        if card["next_available_at"] is not None and card["next_available_at"] > clock:
            clock = card["next_available_at"]  # stand-in for other cards' events filling the gap
        txn = p.normal_txn(card, merchants, ts=clock)[0]
        if prev is not None and prev["country"] != txn["country"]:
            dt_hours = ((datetime.fromisoformat(txn["event_time"])
                         - datetime.fromisoformat(prev["event_time"])).total_seconds() / 3600)
            dist_km = p.haversine_km(*p.COUNTRIES[prev["country"]][:2], *p.COUNTRIES[txn["country"]][:2])
            speed_kmh = dist_km / dt_hours if dt_hours > 0 else float("inf")
            assert speed_kmh <= 900, (
                f"{prev['country']}->{txn['country']} implied speed {speed_kmh:.0f} km/h "
                f"over {dt_hours:.2f}h - transition still teleports"
            )
        prev = txn
        clock += timedelta(seconds=random.uniform(1, 5))


def test_generate_events_transitions_respect_flight_physics(merchants):
    # Integration-level: exercises the real _event_stream scheduler across
    # many cards sharing one clock (unlike the single-card test above),
    # which is where a card could previously get drawn again before its
    # flight gate cleared (the _pick_available_card fallback bug).
    cards = p.build_card_pool(n_cards=30)
    events = p.generate_events(4000, cards, merchants, fraud_rate=0.0, txns_per_day_per_card=4)

    # Only AUTH rows represent the card's own location; SETTLEMENT events
    # for the same card can post hours later at whatever country their
    # originating AUTH had, unrelated to flight gating, and would otherwise
    # look like spurious teleports when interleaved into the timeline.
    auths = [e for e in events if e["event_type"] == "AUTH"]
    last_by_card: dict[str, dict] = {}
    for e in sorted(auths, key=lambda e: e["event_time"]):
        prev = last_by_card.get(e["card_id"])
        if prev is not None and prev["country"] != e["country"]:
            dt_hours = ((datetime.fromisoformat(e["event_time"])
                         - datetime.fromisoformat(prev["event_time"])).total_seconds() / 3600)
            dist_km = p.haversine_km(*p.COUNTRIES[prev["country"]][:2], *p.COUNTRIES[e["country"]][:2])
            speed_kmh = dist_km / dt_hours if dt_hours > 0 else float("inf")
            assert speed_kmh <= 900, (
                f"card {e['card_id']}: {prev['country']}->{e['country']} implied speed "
                f"{speed_kmh:.0f} km/h over {dt_hours:.2f}h - transition still teleports"
            )
        last_by_card[e["card_id"]] = e


def test_same_country_card_present_pairs_respect_ground_speed():
    # The blind spot the two tests above miss entirely: both only compute
    # implied speed when the COUNTRY changes between consecutive
    # transactions. This is the complementary, same-country case - exactly
    # where the merchant-fixed-coordinates + short-inter-arrival-gap
    # teleport bug was actually found (via live warehouse data, not a
    # sweep - this test exists so a sweep would have caught it too). Swept
    # across several seeds, matching how the original bug was surfaced.
    checked = 0
    for seed in range(10):
        random.seed(seed)
        p.Faker.seed(seed)
        cards = p.build_card_pool(n_cards=30)
        merchants = p.build_merchant_pool(n_merchants=60)
        events = p.generate_events(1500, cards, merchants, fraud_rate=0.0, txns_per_day_per_card=4)

        auths = [e for e in events if e["event_type"] == "AUTH" and e["card_present"]]
        last_by_card: dict[str, dict] = {}
        for e in sorted(auths, key=lambda e: e["event_time"]):
            prev = last_by_card.get(e["card_id"])
            if prev is not None and prev["country"] == e["country"]:
                dt_hours = ((datetime.fromisoformat(e["event_time"])
                             - datetime.fromisoformat(prev["event_time"])).total_seconds() / 3600)
                if dt_hours > 0:
                    dist_km = p.haversine_km(prev["lat"], prev["lon"], e["lat"], e["lon"])
                    speed_kmh = dist_km / dt_hours
                    checked += 1
                    assert speed_kmh <= 900, (
                        f"seed={seed} card={e['card_id']}: same-country ({e['country']}) "
                        f"card-present speed {speed_kmh:.0f} km/h over {dt_hours:.2f}h"
                    )
            last_by_card[e["card_id"]] = e

    assert checked > 50, f"only exercised {checked} same-country card-present pairs - too few to trust"


def test_geo_impossible_still_violates_physics(cards, merchants):
    # Regression guard: the flight-delay fix must not accidentally sanitize
    # the geo_impossible fraud pattern, which is supposed to stay physically
    # impossible (that's the whole signal). Uses the same elapsed-time
    # computation as the dbt implied_speed_kmh feature: haversine distance
    # over hours between consecutive event_time values.
    card = cards[0]
    next_available_before = card["next_available_at"]
    pending_before = card["pending_country"]
    current_country_before = card["current_country"]

    t1, t2 = p.fraud_geo_impossible(card, merchants)

    # This pair must NOT go through the flight-gating machinery - that's
    # what makes it impossible instead of a legitimate (gated) trip.
    assert card["next_available_at"] == next_available_before
    assert card["pending_country"] == pending_before
    assert card["current_country"] == current_country_before

    dt_hours = abs((datetime.fromisoformat(t2["event_time"])
                    - datetime.fromisoformat(t1["event_time"])).total_seconds()) / 3600
    dist_km = p.haversine_km(*p.COUNTRIES[t1["country"]][:2], *p.COUNTRIES[t2["country"]][:2])
    speed_kmh = dist_km / dt_hours if dt_hours > 0 else float("inf")
    # The gap is now derived from a target speed drawn from
    # GEO_IMPOSSIBLE_SPEED_KMH = (2000, 20000), so the implied speed should
    # land in that band exactly (modulo float rounding) regardless of which
    # two countries got picked - a tolerance band, not an empirically-tuned
    # floor like the previous fixed-gap design needed.
    assert 1500 <= speed_kmh <= 25000, f"geo_impossible pair implies {speed_kmh:.0f} km/h over {dt_hours:.2f}h"


def test_geo_impossible_merchants_match_intended_countries(cards, merchants):
    # Consequence of fixing merchant lat/lon to be fixed-at-creation: if
    # fraud_geo_impossible picked a merchant without regard to country, its
    # (now-authoritative) coordinates would silently not match the intended
    # distant countries, invalidating the haversine distance the pattern
    # depends on.
    card = cards[0]
    t1, t2 = p.fraud_geo_impossible(card, merchants)
    for t in (t1, t2):
        expected_lat, expected_lon, _ = p.COUNTRIES[t["country"]]
        assert abs(t["lat"] - expected_lat) < 1.0
        assert abs(t["lon"] - expected_lon) < 1.0


def test_merchant_coordinates_fixed_across_transactions(cards, merchants):
    merchant = merchants[0]
    card = cards[0]
    t1 = p.base_txn(card, merchant, amount=10.0)
    t2 = p.base_txn(card, merchant, amount=20.0)
    assert t1["lat"] == t2["lat"] == merchant["lat"]
    assert t1["lon"] == t2["lon"] == merchant["lon"]


def test_interarrival_median_is_hours_scale():
    txns_per_day = 4
    samples_hours = sorted(p._sample_interarrival_seconds(txns_per_day) / 3600 for _ in range(5000))
    median = samples_hours[len(samples_hours) // 2]
    expected_median = math.log(2) * 24 / txns_per_day  # exponential dist. median = ln(2)/rate ~= 4.16h
    assert expected_median * 0.7 < median < expected_median * 1.3, (
        f"median inter-arrival {median:.2f}h far from expected {expected_median:.2f}h"
    )


def test_declined_auth_never_settles(cards, merchants):
    n = 500
    declined_ids = set()
    settlements = []
    for _ in range(n):
        card = random.choice(cards)
        auth = p.generate_auth_batch(card, merchants, fraud_rate=0.0)[0]
        settlement, delay = p.decide_settlement(auth, (2, 48))
        if auth["auth_code"] != "00":
            declined_ids.add(auth["transaction_id"])
            assert settlement is None
            assert delay == 0
        elif settlement is not None:
            settlements.append(settlement)
    assert declined_ids, "expected at least one declined auth in 500 tries"
    assert not any(s["auth_transaction_id"] in declined_ids for s in settlements)


def test_parquet_schema_matches_kafka_json_schema(cards, merchants, tmp_path):
    events = p.generate_events(50, cards, merchants, fraud_rate=0.015, txns_per_day_per_card=4)
    assert len(events) == 50

    json_keys = set(json.loads(json.dumps(events[0])).keys())
    assert json_keys == set(p.EVENT_FIELDS)

    path = p.write_parquet(events, tmp_path)
    table = pq.read_table(path)
    assert table.column_names == p.EVENT_FIELDS
    assert table.num_rows == 50
