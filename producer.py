"""
Synthetic card transaction producer.

Generates realistic card transactions and streams them to the `transactions`
Kafka topic, keyed by card_id (per-card ordering within a partition).

Event model: every transaction starts life as an AUTH. Approved AUTHs
(auth_code == "00") settle ~95% of the time after a real 2-48h delay;
declined AUTHs never settle. A small fraction (~0.5%) of SETTLEMENTs are
orphans that reference no real AUTH (force posts), and settled amounts may
drift up to ~20% from the AUTH amount (tips/FX). AUTH and SETTLEMENT events
share one unified schema (fields that don't apply to a given event type are
null), so the Kafka JSON and the --parquet-out Parquet file are the same
shape.

Unified synthetic clock: every duration in the simulation (per-card
transaction cadence, settlement delay, flight-travel gating) runs at real
scale - a card transacts ~4x/day on average, settlement takes a real 2-48h,
a flight takes a real haversine-distance-based number of hours. A single
min-heap (see _event_stream) always knows exactly what happens next, across
every card, in chronological synthetic order. The only place "compression"
exists at all is Kafka mode's real-time pacing: --time-compression says how
many synthetic seconds equal one real wall-clock second when sleeping
between emits. --parquet-out mode never sleeps, so it just runs the same
synthetic timeline as fast as it can, anchored so it ends near generation
wall time.

Fraud patterns injected (each transaction carries a `fraud_type` label so you
can measure model precision/recall later):
  1. velocity        - burst of 5-10 rapid transactions on one card
  2. geo_impossible  - two transactions minutes apart in distant countries,
                        with the gap derived from a target implied speed so
                        the pattern is impossible by construction
  3. amount_anomaly  - transaction 10-50x the card's typical spend

~1.5% of the stream is fraudulent, roughly matching real-world card fraud
base rates (real rates are lower still, ~0.1%, which is worth mentioning in
interviews when discussing class imbalance).

Usage:
    python producer.py                                             # ~4 txns/day/card, runs forever
    python producer.py --time-compression 60 --count 10000
    python producer.py --txns-per-day-per-card 8 --count 1000
    python producer.py --parquet-out ./out --count 500              # Week 1 bridge test file
"""

import argparse
import heapq
import itertools
import json
import math
import random
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Producer
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

TOPIC = "transactions"
BOOTSTRAP = "localhost:9092"

NEVER_SETTLE_RATE = 0.05       # of approved auths, fraction that never settle
ORPHAN_SETTLEMENT_RATE = 0.005 # independent chance per auth of an orphan settlement
AMOUNT_DRIFT = 0.20            # max settlement amount drift, either direction
SETTLEMENT_DELAY_HOURS = (2, 48)  # real auth->settlement delay range

HOME_COUNTRY_RATE = 0.97       # normal txns that stay in the card's home country
TRAVEL_TRIP_LENGTH = (3, 8)    # consecutive normal txns spent in one foreign country per trip
VELOCITY_BURST_GAP_SECONDS = (5, 30)  # spacing between consecutive events in a velocity burst
GEO_IMPOSSIBLE_SPEED_KMH = (2000, 20000)  # target implied speed a geo_impossible pair is built to hit

FLIGHT_SPEED_KMH = 800         # commercial-flight cruise speed, for travel-delay gating
TRAVEL_DELAY_JITTER = (1.0, 1.5)  # multiplier on pure flight time (boarding/customs/ground time)
GROUND_SPEED_KMH = 80          # ground travel speed ceiling for same-country card-present merchant hops

# --- Reference data -----------------------------------------------------------

# Merchant Category Codes (real MCC values; finance vocab, learn these)
MCC = {
    "5411": "Grocery Stores",
    "5812": "Restaurants",
    "5541": "Gas Stations",
    "5999": "Retail Misc",
    "4111": "Transport/Commuter",
    "5732": "Electronics",
    "7011": "Hotels",
    "4511": "Airlines",
    "5967": "Direct Marketing (high fraud MCC)",
    "6011": "ATM Cash Withdrawal",
}

COUNTRIES = {
    # country: (lat, lon, weight in normal traffic)
    "SG": (1.35, 103.82, 50),
    "MY": (3.14, 101.69, 15),
    "ID": (-6.20, 106.85, 10),
    "TH": (13.75, 100.50, 8),
    "US": (40.71, -74.01, 7),
    "GB": (51.51, -0.13, 5),
    "AU": (-33.87, 151.21, 5),
}

CHANNELS = ["POS", "ONLINE", "ATM", "CONTACTLESS"]
CURRENCY_BY_COUNTRY = {"SG": "SGD", "MY": "MYR", "ID": "IDR", "TH": "THB",
                       "US": "USD", "GB": "GBP", "AU": "AUD"}

# ISO-8583-style auth response codes. "00" = approved; ~4% of auths decline.
AUTH_CODES = ["00", "05", "51", "14", "61"]
AUTH_CODE_WEIGHTS = [96, 1, 1, 1, 1]

# Unified event schema: every emitted record has exactly these fields, in this
# order. Fields that don't apply to a given event_type are null. This is what
# keeps the Kafka JSON and the Parquet file byte-for-byte the same shape.
EVENT_FIELDS = [
    "transaction_id", "event_type", "event_time", "auth_transaction_id",
    "card_id", "merchant_id", "merchant_name", "mcc", "mcc_description",
    "amount", "currency", "country", "lat", "lon", "channel",
    "pos_entry_mode", "card_present", "auth_code", "is_fraud", "fraud_type",
]

PARQUET_SCHEMA = pa.schema([
    ("transaction_id", pa.string()),
    ("event_type", pa.string()),
    ("event_time", pa.string()),
    ("auth_transaction_id", pa.string()),
    ("card_id", pa.string()),
    ("merchant_id", pa.string()),
    ("merchant_name", pa.string()),
    ("mcc", pa.string()),
    ("mcc_description", pa.string()),
    ("amount", pa.float64()),
    ("currency", pa.string()),
    ("country", pa.string()),
    ("lat", pa.float64()),
    ("lon", pa.float64()),
    ("channel", pa.string()),
    ("pos_entry_mode", pa.string()),
    ("card_present", pa.bool_()),
    ("auth_code", pa.string()),
    ("is_fraud", pa.int8()),
    ("fraud_type", pa.string()),
])


def build_card_pool(n_cards: int = 500) -> list[dict]:
    """Each card gets a home country, a typical spend level, and preferred merchants.
    Card-level behavioral baselines are what make anomalies detectable.

    current_country/travel_remaining/pending_country/next_available_at are
    mutable trip state (see _normal_txn_country): a fresh card is home-bound
    (current_country == home_country) with no pending flight.

    last_present_merchant/last_present_at track the card's most recent
    card-present (physical) transaction, for ground-speed-limited merchant
    selection (see _pick_reachable_merchant): a fresh card has no such
    history yet.
    """
    cards = []
    for _ in range(n_cards):
        home = random.choices(list(COUNTRIES), weights=[w for _, _, w in COUNTRIES.values()])[0]
        cards.append({
            "card_id": f"card_{uuid.uuid4().hex[:12]}",
            "home_country": home,
            "avg_spend": round(random.lognormvariate(3.5, 0.8), 2),  # median ~SGD 33
            "preferred_mccs": random.sample(list(MCC), k=4),
            "current_country": home,
            "travel_remaining": 0,
            "pending_country": None,
            "next_available_at": None,
            "last_present_merchant": None,
            "last_present_at": None,
        })
    return cards


def build_merchant_pool(n_merchants: int = 300) -> list[dict]:
    """Each merchant gets fixed lat/lon at creation (country reference point
    plus one-time jitter) - a merchant is a physical place, so every
    transaction at it should carry exactly the same coordinates."""
    merchants = []
    for _ in range(n_merchants):
        country = random.choices(list(COUNTRIES), weights=[w for _, _, w in COUNTRIES.values()])[0]
        lat, lon, _ = COUNTRIES[country]
        merchants.append({
            "merchant_id": f"m_{uuid.uuid4().hex[:10]}",
            "merchant_name": fake.company(),
            "mcc": random.choice(list(MCC)),
            "country": country,
            "lat": round(lat + random.uniform(-0.3, 0.3), 4),
            "lon": round(lon + random.uniform(-0.3, 0.3), 4),
        })
    return merchants


# --- Transaction generation ---------------------------------------------------

def derive_entry_fields(channel: str) -> tuple[str, bool]:
    """POS entry mode + card-present flag, derived from channel."""
    if channel == "ONLINE":
        return "ECOMMERCE", False
    if channel == "CONTACTLESS":
        return "CONTACTLESS", True
    if channel == "ATM":
        return "CHIP", True
    return random.choices(["CHIP", "SWIPE", "MANUAL"], weights=[80, 15, 5])[0], True


def derive_auth_code() -> str:
    return random.choices(AUTH_CODES, weights=AUTH_CODE_WEIGHTS)[0]


def base_txn(card: dict, merchant: dict, amount: float, ts: datetime | None = None,
             channel: str | None = None, country: str | None = None) -> dict:
    country = country or merchant["country"]
    channel = channel or random.choices(CHANNELS, weights=[45, 30, 10, 15])[0]
    pos_entry_mode, card_present = derive_entry_fields(channel)
    return {
        "transaction_id": str(uuid.uuid4()),
        "event_type": "AUTH",
        "event_time": (ts or datetime.now(timezone.utc)).isoformat(),
        "auth_transaction_id": None,
        "card_id": card["card_id"],
        "merchant_id": merchant["merchant_id"],
        "merchant_name": merchant["merchant_name"],
        "mcc": merchant["mcc"],
        "mcc_description": MCC[merchant["mcc"]],
        "amount": round(amount, 2),
        "currency": CURRENCY_BY_COUNTRY[country],
        "country": country,
        "lat": merchant["lat"],
        "lon": merchant["lon"],
        "channel": channel,
        "pos_entry_mode": pos_entry_mode,
        "card_present": card_present,
        "auth_code": derive_auth_code(),
        "is_fraud": 0,
        "fraud_type": None,
    }


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in km. Python mirror of dbt/macros/haversine.sql,
    used here to gate plausible card travel time between countries."""
    r = 6371
    lat1, lon1, lat2, lon2 = map(math.radians, (lat1, lon1, lat2, lon2))
    a = (math.sin((lat2 - lat1) / 2) ** 2
         + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2)
    return 2 * r * math.asin(math.sqrt(a))


def _flight_delay_hours(from_country: str, to_country: str) -> float:
    """Plausible travel time between two countries' reference points, at
    FLIGHT_SPEED_KMH plus TRAVEL_DELAY_JITTER for boarding/customs/ground time.
    Always real hours - no compression concept lives at this layer."""
    lat1, lon1, _ = COUNTRIES[from_country]
    lat2, lon2, _ = COUNTRIES[to_country]
    return haversine_km(lat1, lon1, lat2, lon2) / FLIGHT_SPEED_KMH * random.uniform(*TRAVEL_DELAY_JITTER)


def _normal_txn_country(card: dict, event_time: datetime) -> str:
    """Country for this card's next normal transaction. HOME_COUNTRY_RATE of
    the time it stays home; otherwise the card enters "travel mode" for a
    TRAVEL_TRIP_LENGTH run of consecutive normal transactions in one foreign
    country before returning home.

    Cards go quiet while they "fly": a country change (trip start or trip
    return) doesn't take effect on the transaction that decides it - that
    transaction stays in the card's current country, and next_available_at
    is pushed out by a haversine-distance-based real flight delay. The
    per-card scheduler (_event_stream) never schedules this card's next turn
    before next_available_at, so the next time this card actually transacts,
    the pending country becomes current - meaning the transition row itself
    is correctly separated from the last pre-flight row by a physically
    plausible elapsed time, not just whatever the next draw happened to be.
    """
    if card["pending_country"] is not None:
        card["current_country"] = card["pending_country"]
        card["pending_country"] = None
        return card["current_country"]

    if card["travel_remaining"] > 0:
        card["travel_remaining"] -= 1
        return card["current_country"]

    if random.random() < HOME_COUNTRY_RATE:
        target, trip_length = card["home_country"], 0
    else:
        foreign = [c for c in COUNTRIES if c != card["home_country"]]
        target = random.choice(foreign)
        trip_length = random.randint(*TRAVEL_TRIP_LENGTH) - 1  # this txn's arrival is trip txn #1

    if target != card["current_country"]:
        delay_hours = _flight_delay_hours(card["current_country"], target)
        card["next_available_at"] = event_time + timedelta(hours=delay_hours)
        card["pending_country"] = target
    card["travel_remaining"] = trip_length
    return card["current_country"]


def _pick_reachable_merchant(card: dict, eligible: list[dict], target_country: str,
                              event_time: datetime) -> dict:
    """Card-present merchant selection respects ground travel: pick among
    merchants reachable at GROUND_SPEED_KMH given elapsed time since the
    card's last card-present event, falling back to that same merchant if
    nothing in range (never teleport a physically-present card across a
    country in zero time just because the merchant pool is large).

    No constraint (free pick) when there's no prior card-present event, or
    when the last one was in a different country - a country change is
    already physics-gated separately by flight delay (_normal_txn_country),
    on a wholly different, much larger timescale than ground travel between
    merchants within one country.
    """
    last_merchant = card["last_present_merchant"]
    last_at = card["last_present_at"]
    if last_merchant is None or last_merchant["country"] != target_country:
        return random.choice(eligible)

    elapsed_hours = max((event_time - last_at).total_seconds() / 3600, 0)
    max_reachable_km = elapsed_hours * GROUND_SPEED_KMH
    reachable = [m for m in eligible
                 if haversine_km(last_merchant["lat"], last_merchant["lon"], m["lat"], m["lon"])
                 <= max_reachable_km]
    return random.choice(reachable) if reachable else last_merchant


def normal_txn(card: dict, merchants: list[dict], ts: datetime | None = None) -> list[dict]:
    event_time = ts or datetime.now(timezone.utc)
    country = _normal_txn_country(card, event_time)
    channel = random.choices(CHANNELS, weights=[45, 30, 10, 15])[0]
    is_present = channel != "ONLINE"  # matches derive_entry_fields' own ONLINE -> card_present=False rule
    eligible = [m for m in merchants if m["country"] == country] or merchants

    if is_present:
        merchant = _pick_reachable_merchant(card, eligible, country, event_time)
        card["last_present_merchant"] = merchant
        card["last_present_at"] = event_time
    else:
        merchant = random.choice(eligible)  # CNP: no location to be reachable from

    # spend near the card's baseline, log-normal noise
    amount = max(1.0, random.lognormvariate(0, 0.6) * card["avg_spend"])
    return [base_txn(card, merchant, amount, event_time, channel=channel, country=country)]


def fraud_velocity(card: dict, merchants: list[dict], ts: datetime | None = None) -> list[dict]:
    """5-10 small-to-medium transactions on one card and merchant, spaced
    VELOCITY_BURST_GAP_SECONDS apart (up to ~4.5 min end-to-end for a 10-txn
    burst) so implied_speed_kmh-style elapsed-time features see a real,
    strictly-ascending timeline instead of a same-second pile-up. Classic
    card-testing / stolen-card cashout pattern."""
    txns = []
    current_ts = ts or datetime.now(timezone.utc)
    merchant = random.choice(merchants)
    for i in range(random.randint(5, 10)):
        if i > 0:
            current_ts += timedelta(seconds=random.uniform(*VELOCITY_BURST_GAP_SECONDS))
        amount = random.uniform(1, 3) * card["avg_spend"]
        t = base_txn(card, merchant, amount, current_ts, channel="ONLINE")
        t["is_fraud"], t["fraud_type"] = 1, "velocity"
        txns.append(t)
    return txns


def fraud_geo_impossible(card: dict, merchants: list[dict], ts: datetime | None = None) -> list[dict]:
    """Two transactions in distant countries, with the gap derived from a
    target implied speed (GEO_IMPOSSIBLE_SPEED_KMH) rather than a fixed time
    window - gap_hours = haversine_km(c1, c2) / target_speed_kmh guarantees
    the pattern is impossible BY CONSTRUCTION regardless of which two
    countries get picked, unlike a fixed gap (which could coincidentally
    look plausible for nearby countries). Bypasses flight gating entirely
    (no _normal_txn_country involvement) - that's what makes it impossible
    instead of a legitimate (gated) trip. Merchants are filtered by country
    so the merchant-fixed lat/lon actually reflects the intended distant
    countries, not a random merchant's location."""
    base_ts = ts or datetime.now(timezone.utc)
    c1, c2 = random.sample(list(COUNTRIES), 2)
    dist_km = haversine_km(*COUNTRIES[c1][:2], *COUNTRIES[c2][:2])
    target_speed_kmh = random.uniform(*GEO_IMPOSSIBLE_SPEED_KMH)
    gap_hours = dist_km / target_speed_kmh

    txns = []
    for i, country in enumerate((c1, c2)):
        event_time = base_ts if i == 0 else base_ts + timedelta(hours=gap_hours)
        eligible = [m for m in merchants if m["country"] == country] or merchants
        merchant = random.choice(eligible)
        amount = random.uniform(0.5, 4) * card["avg_spend"]
        t = base_txn(card, merchant, amount, event_time, channel="POS", country=country)
        t["is_fraud"], t["fraud_type"] = 1, "geo_impossible"
        txns.append(t)
    return txns


def fraud_amount_anomaly(card: dict, merchants: list[dict], ts: datetime | None = None) -> list[dict]:
    """Single transaction 10-50x the card's typical spend, often high-risk MCC."""
    merchant = random.choice([m for m in merchants if m["mcc"] in ("5967", "5732", "4511")]
                             or merchants)
    amount = random.uniform(10, 50) * card["avg_spend"]
    t = base_txn(card, merchant, amount, ts, channel="ONLINE")
    t["is_fraud"], t["fraud_type"] = 1, "amount_anomaly"
    return [t]


FRAUD_GENERATORS = [fraud_velocity, fraud_geo_impossible, fraud_amount_anomaly]


def generate_auth_batch(card: dict, merchants: list[dict], fraud_rate: float,
                         ts: datetime | None = None) -> list[dict]:
    """One card's worth of AUTH events for this turn: fraud with probability
    fraud_rate (via one of the three generators), else a normal transaction."""
    if random.random() < fraud_rate:
        return random.choice(FRAUD_GENERATORS)(card, merchants, ts)
    return normal_txn(card, merchants, ts)


# --- Settlement generation ------------------------------------------------------

def decide_settlement(auth: dict, delay_hours_range: tuple[float, float] = SETTLEMENT_DELAY_HOURS
                       ) -> tuple[dict | None, float]:
    """Decide if/when/how an AUTH settles.

    Declined auths (auth_code != "00") never settle. Approved auths settle
    ~95% of the time (NEVER_SETTLE_RATE), after a delay drawn uniformly from
    delay_hours_range real hours, with the amount drifted up to AMOUNT_DRIFT
    (tips/FX). Returns (settlement_dict_or_None, delay_hours) - delay_hours
    is 0 when there is no settlement.
    """
    if auth["auth_code"] != "00":
        return None, 0
    if random.random() < NEVER_SETTLE_RATE:
        return None, 0

    delay_hours = random.uniform(*delay_hours_range)
    auth_time = datetime.fromisoformat(auth["event_time"])
    settle_time = auth_time + timedelta(hours=delay_hours)
    drift = random.uniform(-AMOUNT_DRIFT, AMOUNT_DRIFT)
    amount = round(auth["amount"] * (1 + drift), 2)

    settlement = {field: None for field in EVENT_FIELDS}
    settlement.update({
        "transaction_id": str(uuid.uuid4()),
        "event_type": "SETTLEMENT",
        "event_time": settle_time.isoformat(),
        "auth_transaction_id": auth["transaction_id"],
        "card_id": auth["card_id"],
        "merchant_id": auth["merchant_id"],
        "merchant_name": auth["merchant_name"],
        "mcc": auth["mcc"],
        "mcc_description": auth["mcc_description"],
        "amount": amount,
        "currency": auth["currency"],
        "country": auth["country"],
        "is_fraud": auth["is_fraud"],
        "fraud_type": auth["fraud_type"],
    })
    return settlement, delay_hours


def orphan_settlement(cards: list[dict], merchants: list[dict], ts: datetime | None = None) -> dict:
    """A force-post: a SETTLEMENT with no matching AUTH in the stream."""
    card = random.choice(cards)
    merchant = random.choice(merchants)
    amount = max(1.0, random.lognormvariate(0, 0.6) * card["avg_spend"])

    settlement = {field: None for field in EVENT_FIELDS}
    settlement.update({
        "transaction_id": str(uuid.uuid4()),
        "event_type": "SETTLEMENT",
        "event_time": (ts or datetime.now(timezone.utc)).isoformat(),
        "auth_transaction_id": str(uuid.uuid4()),  # fabricated, matches no real AUTH
        "card_id": card["card_id"],
        "merchant_id": merchant["merchant_id"],
        "merchant_name": merchant["merchant_name"],
        "mcc": merchant["mcc"],
        "mcc_description": MCC[merchant["mcc"]],
        "amount": round(amount, 2),
        "currency": CURRENCY_BY_COUNTRY[merchant["country"]],
        "country": merchant["country"],
        "is_fraud": 0,
        "fraud_type": None,
    })
    return settlement


# --- Unified event-driven scheduling --------------------------------------------

def _sample_interarrival_seconds(txns_per_day_per_card: float) -> float:
    """Exponential inter-arrival time for a Poisson arrival process at the
    given per-card daily rate."""
    rate_per_second = txns_per_day_per_card / 86400
    return random.expovariate(rate_per_second)


def _event_stream(cards: list[dict], merchants: list[dict], fraud_rate: float,
                   txns_per_day_per_card: float, settlement_delay_hours: tuple[float, float],
                   start_time: datetime, heap: list[tuple[datetime, int, str, dict]] | None = None):
    """Core simulation loop, shared by Kafka and Parquet modes. Yields
    (event, synthetic_time, kind) forever, in strict chronological synthetic
    order - kind is "auth" | "settlement" | "orphan".

    A single min-heap drives everything: each card has its own Poisson
    arrival process (exponential inter-arrival at txns_per_day_per_card);
    settlements and orphan force-posts are scheduled onto the SAME heap
    using each event's own timestamp, so the whole stream - auths,
    settlements, orphans, across every card - comes out in one strictly
    ordered synthetic timeline. A card's next turn is never scheduled before
    its flight gate (next_available_at, see _normal_txn_country) clears, so
    there's no separate "is this card available" filter needed at pop time.

    Pass a `heap` list in if the caller wants to inspect what's still
    pending after the generator is abandoned mid-stream (e.g. Ctrl+C) - the
    generator only ever pops/pushes onto it, never drains it on its own.
    """
    if heap is None:
        heap = []
    seq = itertools.count()  # tie-breaker so heapq never compares dicts

    for card in cards:
        due = start_time + timedelta(seconds=_sample_interarrival_seconds(txns_per_day_per_card))
        heapq.heappush(heap, (due, next(seq), "txn", card))

    while True:
        due_at, _, kind, payload = heapq.heappop(heap)

        if kind != "txn":
            yield payload, due_at, kind
            continue

        card = payload
        last_event_time = due_at
        for a in generate_auth_batch(card, merchants, fraud_rate, ts=due_at):
            a_time = datetime.fromisoformat(a["event_time"])
            last_event_time = max(last_event_time, a_time)
            yield a, a_time, "auth"

            settlement, delay_hours = decide_settlement(a, settlement_delay_hours)
            if settlement is not None:
                settle_at = a_time + timedelta(hours=delay_hours)
                heapq.heappush(heap, (settle_at, next(seq), "settlement", settlement))

            if random.random() < ORPHAN_SETTLEMENT_RATE:
                orphan = orphan_settlement(cards, merchants, ts=a_time)
                orphan_at = a_time + timedelta(minutes=random.uniform(0, 5))
                heapq.heappush(heap, (orphan_at, next(seq), "orphan", orphan))

        next_due = last_event_time + timedelta(seconds=_sample_interarrival_seconds(txns_per_day_per_card))
        if card["next_available_at"] is not None and card["next_available_at"] > next_due:
            next_due = card["next_available_at"]
        heapq.heappush(heap, (next_due, next(seq), "txn", card))


# --- Kafka plumbing -----------------------------------------------------------

def delivery_report(err, msg):
    if err is not None:
        print(f"DELIVERY FAILED: {err}")


def emit(producer: Producer, event: dict) -> None:
    producer.produce(
        TOPIC,
        key=event["card_id"].encode(),  # per-card ordering
        value=json.dumps(event).encode(),
        callback=delivery_report,
    )


def run_kafka(args: argparse.Namespace) -> None:
    producer = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "enable.idempotence": True,   # producer-side dedupe on retries
        "acks": "all",
        "linger.ms": 20,
    })

    cards = build_card_pool()
    merchants = build_merchant_pool()
    start_time = datetime.now(timezone.utc)  # live stream: clock starts now, moves forward
    heap: list[tuple[datetime, int, str, dict]] = []  # kept so Ctrl+C can inspect what's still pending
    stream = _event_stream(cards, merchants, args.fraud_rate, args.txns_per_day_per_card,
                            SETTLEMENT_DELAY_HOURS, start_time, heap=heap)

    sent = auth_sent = fraud_sent = settled_sent = orphan_sent = 0
    prev_synthetic = start_time
    interrupted = False

    print(f"Producing to '{TOPIC}': {args.txns_per_day_per_card} txns/day/card x {len(cards)} cards, "
          f"{args.time_compression}x time compression. Ctrl+C to stop.")
    try:
        for event, due_at, kind in stream:
            if args.count and sent >= args.count:
                break

            real_wait = (due_at - prev_synthetic).total_seconds() / args.time_compression
            if real_wait > 0:
                time.sleep(real_wait)

            emit(producer, event)
            sent += 1
            prev_synthetic = due_at
            if kind == "auth":
                auth_sent += 1
                fraud_sent += event["is_fraud"]
            elif kind == "settlement":
                settled_sent += 1
            elif kind == "orphan":
                orphan_sent += 1

            producer.poll(0)
            if sent % 100 == 0:
                print(f"sent={sent}  fraud={fraud_sent}  settled={settled_sent}  "
                      f"orphans={orphan_sent}  synthetic_time={due_at.isoformat()}  "
                      f"fraud_rate={fraud_sent / max(sent, 1):.2%}")
    except KeyboardInterrupt:
        interrupted = True
    finally:
        producer.flush()
        print(f"\nDone. sent={sent}, fraud={fraud_sent}, settled={settled_sent}, orphans={orphan_sent}")
        if interrupted:
            # No drain on interrupt - a real system stopping doesn't
            # retroactively settle everything either. This just reports
            # what was left in flight, on the heap, untouched.
            pending = sum(1 for entry in heap if entry[2] != "txn")
            gated_cards = sum(1 for c in cards
                               if c["next_available_at"] is not None and c["next_available_at"] > prev_synthetic)
            print(f"Interrupted: {auth_sent} auths emitted, {pending} settlements/orphans still "
                  f"pending on the heap, {gated_cards} cards currently flight-gated.")


# --- Parquet bridge-test mode ---------------------------------------------------

def generate_events(n: int, cards: list[dict], merchants: list[dict], fraud_rate: float,
                     txns_per_day_per_card: float,
                     settlement_delay_hours: tuple[float, float] = SETTLEMENT_DELAY_HOURS) -> list[dict]:
    """Generate n events via the unified synthetic-clock event stream. The
    start time is anchored so the run's span (estimated from the aggregate
    per-card arrival rate) ends near generation wall time - this is a
    stochastic Poisson process, so the last event lands approximately, not
    exactly, at "now"."""
    aggregate_rate_per_second = len(cards) * txns_per_day_per_card / 86400
    expected_span_seconds = n / aggregate_rate_per_second
    start_time = datetime.now(timezone.utc) - timedelta(seconds=expected_span_seconds)

    stream = _event_stream(cards, merchants, fraud_rate, txns_per_day_per_card,
                            settlement_delay_hours, start_time)
    events: list[dict] = []
    for event, _, _ in stream:
        events.append(event)
        if len(events) >= n:
            break

    events.sort(key=lambda e: e["event_time"])
    return events[:n]


def write_parquet(events: list[dict], out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(events, schema=PARQUET_SCHEMA)
    path = out_dir / f"transactions_{datetime.now(timezone.utc):%Y%m%dT%H%M%S}.parquet"
    pq.write_table(table, path)
    return path


def run_parquet(args: argparse.Namespace) -> None:
    cards = build_card_pool()
    merchants = build_merchant_pool()
    events = generate_events(args.count, cards, merchants, args.fraud_rate, args.txns_per_day_per_card)
    path = write_parquet(events, Path(args.parquet_out))

    fraud = sum(e["is_fraud"] for e in events)
    settlements = sum(1 for e in events if e["event_type"] == "SETTLEMENT")
    print(f"Wrote {len(events)} events ({fraud} fraud, {settlements} settlements) to {path}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=0, help="stop after N transactions (0 = forever; required > 0 for --parquet-out)")
    parser.add_argument("--fraud-rate", type=float, default=0.015)
    parser.add_argument("--txns-per-day-per-card", type=float, default=4,
                         help="mean transactions per card per day, drawn as an exponential inter-arrival process")
    parser.add_argument("--time-compression", type=float, default=600,
                         help="Kafka mode only: synthetic seconds per real wall-clock second; "
                              "default 600 means 1 wall second = 10 synthetic minutes. "
                              "--parquet-out mode never sleeps, so this doesn't apply to it.")
    parser.add_argument("--parquet-out", type=str, default=None,
                         help="write --count events to one Parquet file in this dir instead of producing to Kafka")
    args = parser.parse_args()

    if args.parquet_out:
        if args.count <= 0:
            parser.error("--parquet-out requires --count > 0")
        run_parquet(args)
    else:
        run_kafka(args)


if __name__ == "__main__":
    main()
