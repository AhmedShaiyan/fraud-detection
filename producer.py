"""
Synthetic card transaction producer.

Generates realistic card transactions and streams them to the `transactions`
Kafka topic, keyed by card_id (per-card ordering within a partition).

Event model: every transaction starts life as an AUTH, emitted immediately.
Approved AUTHs (auth_code == "00") settle ~95% of the time after a
configurable, compressed delay (real-world settlement is 2-48h later);
declined AUTHs never settle. A small fraction (~0.5%) of SETTLEMENTs are
orphans that reference no real AUTH (force posts), and settled amounts may
drift up to ~20% from the AUTH amount (tips/FX). AUTH and SETTLEMENT events
share one unified schema (fields that don't apply to a given event type are
null), so the Kafka JSON and the --parquet-out Parquet file are the same
shape.

Fraud patterns injected (each transaction carries a `fraud_type` label so you
can measure model precision/recall later):
  1. velocity        - burst of 5-10 rapid transactions on one card
  2. geo_impossible  - two transactions minutes apart in distant countries
  3. amount_anomaly  - transaction 10-50x the card's typical spend

~1.5% of the stream is fraudulent, roughly matching real-world card fraud
base rates (real rates are lower still, ~0.1%, which is worth mentioning in
interviews when discussing class imbalance).

Usage:
    python producer.py --rate 10                                  # ~10 txns/sec, runs forever
    python producer.py --rate 50 --count 10000
    python producer.py --rate 20 --count 1000 --settlement-delay-mins 2 10
    python producer.py --parquet-out ./out --count 500            # Week 1 bridge test file
"""

import argparse
import heapq
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

HOME_COUNTRY_RATE = 0.97       # normal txns that stay in the card's home country
TRAVEL_TRIP_LENGTH = (3, 8)    # consecutive normal txns spent in one foreign country per trip
VELOCITY_BURST_GAP_SECONDS = (5, 30)  # spacing between consecutive events in a velocity burst
GEO_IMPOSSIBLE_GAP_MINUTES = (2, 10)  # spacing between the two events in a geo_impossible pair

FLIGHT_SPEED_KMH = 800         # commercial-flight cruise speed, for travel-delay gating
TRAVEL_DELAY_JITTER = (1.0, 1.5)  # multiplier on pure flight time (boarding/customs/ground time)

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
        })
    return cards


def build_merchant_pool(n_merchants: int = 300) -> list[dict]:
    merchants = []
    for _ in range(n_merchants):
        country = random.choices(list(COUNTRIES), weights=[w for _, _, w in COUNTRIES.values()])[0]
        merchants.append({
            "merchant_id": f"m_{uuid.uuid4().hex[:10]}",
            "merchant_name": fake.company(),
            "mcc": random.choice(list(MCC)),
            "country": country,
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
    lat, lon, _ = COUNTRIES[country]
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
        "lat": round(lat + random.uniform(-0.3, 0.3), 4),
        "lon": round(lon + random.uniform(-0.3, 0.3), 4),
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
    FLIGHT_SPEED_KMH plus TRAVEL_DELAY_JITTER for boarding/customs/ground time."""
    lat1, lon1, _ = COUNTRIES[from_country]
    lat2, lon2, _ = COUNTRIES[to_country]
    return haversine_km(lat1, lon1, lat2, lon2) / FLIGHT_SPEED_KMH * random.uniform(*TRAVEL_DELAY_JITTER)


def _normal_txn_country(card: dict, event_time: datetime, compression: float = 1.0) -> str:
    """Country for this card's next normal transaction. HOME_COUNTRY_RATE of
    the time it stays home; otherwise the card enters "travel mode" for a
    TRAVEL_TRIP_LENGTH run of consecutive normal transactions in one foreign
    country before returning home.

    Cards go quiet while they "fly": a country change (trip start or trip
    return) doesn't take effect on the transaction that decides it - that
    transaction stays in the card's current country, and next_available_at
    is pushed out by a haversine-distance-based flight delay (divided by
    `compression` for demo-scale Kafka runs). _pick_available_card skips the
    card until that time, so the next time this card is actually drawn, the
    pending country becomes current - meaning the transition row itself is
    correctly separated from the last pre-flight row by a physically
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
        delay_hours = _flight_delay_hours(card["current_country"], target) / compression
        card["next_available_at"] = event_time + timedelta(hours=delay_hours)
        card["pending_country"] = target
    card["travel_remaining"] = trip_length
    return card["current_country"]


def _pick_available_card(cards: list[dict], now: datetime) -> dict | None:
    """Random card that isn't mid-flight (see _normal_txn_country). Returns
    None if every card happens to be traveling at once - the caller must
    skip this tick (or fast-forward its clock) rather than pick a
    still-gated card anyway, which would silently defeat the flight gate."""
    available = [c for c in cards if c["next_available_at"] is None or c["next_available_at"] <= now]
    return random.choice(available) if available else None


def normal_txn(card: dict, merchants: list[dict], ts: datetime | None = None,
               compression: float = 1.0) -> list[dict]:
    event_time = ts or datetime.now(timezone.utc)
    country = _normal_txn_country(card, event_time, compression)
    eligible = [m for m in merchants if m["country"] == country] or merchants
    merchant = random.choice(eligible)
    # spend near the card's baseline, log-normal noise
    amount = max(1.0, random.lognormvariate(0, 0.6) * card["avg_spend"])
    return [base_txn(card, merchant, amount, event_time, country=country)]


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
    """Two transactions GEO_IMPOSSIBLE_GAP_MINUTES apart in countries too far
    to travel between in that time (unlike normal_txn's travel gating, this
    pair does NOT go through _pick_available_card/next_available_at - the
    whole point is that these two rows are impossible, not that the card was
    smoothly unavailable in between)."""
    current_ts = ts or datetime.now(timezone.utc)
    c1, c2 = random.sample(list(COUNTRIES), 2)
    txns = []
    for i, country in enumerate((c1, c2)):
        if i > 0:
            current_ts += timedelta(minutes=random.uniform(*GEO_IMPOSSIBLE_GAP_MINUTES))
        merchant = random.choice(merchants)
        amount = random.uniform(0.5, 4) * card["avg_spend"]
        t = base_txn(card, merchant, amount, current_ts, channel="POS", country=country)
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
                         ts: datetime | None = None, compression: float = 1.0) -> list[dict]:
    """One card's worth of AUTH events for this tick: fraud with probability
    fraud_rate (via one of the three generators), else a normal transaction."""
    if random.random() < fraud_rate:
        return random.choice(FRAUD_GENERATORS)(card, merchants, ts)
    return normal_txn(card, merchants, ts, compression)


# --- Settlement generation ------------------------------------------------------

def decide_settlement(auth: dict, delay_range: tuple[float, float]) -> tuple[dict | None, float]:
    """Decide if/when/how an AUTH settles.

    Declined auths (auth_code != "00") never settle. Approved auths settle
    ~95% of the time (NEVER_SETTLE_RATE), after a delay drawn uniformly from
    delay_range minutes, with the amount drifted up to AMOUNT_DRIFT (tips/FX).
    Returns (settlement_dict_or_None, delay_minutes) - delay_minutes is 0 when
    there is no settlement.
    """
    if auth["auth_code"] != "00":
        return None, 0
    if random.random() < NEVER_SETTLE_RATE:
        return None, 0

    delay_minutes = random.uniform(*delay_range)
    auth_time = datetime.fromisoformat(auth["event_time"])
    settle_time = auth_time + timedelta(minutes=delay_minutes)
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
    return settlement, delay_minutes


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
    pending: list[tuple[float, dict]] = []  # heap of (due_at wall-clock, settlement event)
    sent = fraud_sent = settled_sent = orphan_sent = 0

    print(f"Producing to '{TOPIC}' at ~{args.rate}/sec. Ctrl+C to stop.")
    try:
        while args.count == 0 or sent < args.count:
            now = time.time()
            while pending and pending[0][0] <= now:
                _, settlement = heapq.heappop(pending)
                emit(producer, settlement)
                sent += 1
                settled_sent += 1

            card = _pick_available_card(cards, datetime.now(timezone.utc))
            if card is None:
                # every card is mid-flight; let real wall-clock time pass
                # rather than pick a still-gated card and fake its arrival
                producer.poll(0)
                time.sleep(0.05)
                continue
            auths = generate_auth_batch(card, merchants, args.fraud_rate,
                                         compression=args.travel_delay_compression)

            for a in auths:
                emit(producer, a)
                sent += 1
                fraud_sent += a["is_fraud"]

                settlement, delay_minutes = decide_settlement(a, args.settlement_delay_mins)
                if settlement is not None:
                    heapq.heappush(pending, (time.time() + delay_minutes * 60, settlement))

                if random.random() < ORPHAN_SETTLEMENT_RATE:
                    orphan = orphan_settlement(cards, merchants)
                    heapq.heappush(pending, (time.time() + random.uniform(0, 5), orphan))
                    orphan_sent += 1

            producer.poll(0)
            if sent % 100 < len(auths):
                print(f"sent={sent}  fraud={fraud_sent}  settled={settled_sent}  "
                      f"orphans={orphan_sent}  pending={len(pending)}  "
                      f"fraud_rate={fraud_sent / max(sent, 1):.2%}")
            time.sleep(len(auths) / args.rate)
    except KeyboardInterrupt:
        pass
    finally:
        if pending:
            print(f"Fast-forwarding {len(pending)} pending settlements...")
            while pending:
                _, settlement = heapq.heappop(pending)
                emit(producer, settlement)
                sent += 1
                settled_sent += 1
            producer.poll(0)
        producer.flush()
        print(f"\nDone. sent={sent}, fraud={fraud_sent}, settled={settled_sent}, orphans={orphan_sent}")


# --- Parquet bridge-test mode ---------------------------------------------------

def generate_events(n: int, cards: list[dict], merchants: list[dict],
                     fraud_rate: float, delay_range: tuple[float, float]) -> list[dict]:
    """Generate n events (AUTH + their resolved SETTLEMENT/orphans) with a
    synthetic advancing clock instead of real-time waiting, for the Parquet
    bridge-test file."""
    events: list[dict] = []
    clock = datetime.now(timezone.utc)

    while len(events) < n:
        card = _pick_available_card(cards, clock)
        if card is None:
            # every card is mid-flight; fast-forward the synthetic clock to
            # the soonest one that lands rather than manufacture a
            # transaction that would violate its own flight gate.
            clock = min(c["next_available_at"] for c in cards)
            continue
        # compression=1.0 (default): travel delays are enforced against this
        # synthetic clock directly, at real flight-time scale - no wall-clock
        # demo constraint to compress for, unlike Kafka mode.
        auths = generate_auth_batch(card, merchants, fraud_rate, ts=clock)

        for a in auths:
            events.append(a)
            settlement, _ = decide_settlement(a, delay_range)
            if settlement is not None:
                events.append(settlement)
            if random.random() < ORPHAN_SETTLEMENT_RATE:
                events.append(orphan_settlement(cards, merchants, ts=clock))

        clock += timedelta(seconds=random.uniform(0.1, 2.0) * len(auths))

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
    events = generate_events(args.count, cards, merchants, args.fraud_rate, args.settlement_delay_mins)
    path = write_parquet(events, Path(args.parquet_out))

    fraud = sum(e["is_fraud"] for e in events)
    settlements = sum(1 for e in events if e["event_type"] == "SETTLEMENT")
    print(f"Wrote {len(events)} events ({fraud} fraud, {settlements} settlements) to {path}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=float, default=10, help="approx transactions per second")
    parser.add_argument("--count", type=int, default=0, help="stop after N transactions (0 = forever; required > 0 for --parquet-out)")
    parser.add_argument("--fraud-rate", type=float, default=0.015)
    parser.add_argument("--settlement-delay-mins", type=float, nargs=2, default=[2, 10],
                         metavar=("MIN", "MAX"), help="compressed settlement delay range in minutes")
    parser.add_argument("--travel-delay-compression", type=float, default=200,
                         help="Kafka mode only: divides real flight time (haversine distance / "
                              "%d km/h) down to demo wall-clock scale, e.g. default 200 means a "
                              "17h flight takes ~5min wall time. --parquet-out mode always uses "
                              "the uncompressed real flight time against its synthetic clock." % FLIGHT_SPEED_KMH)
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
