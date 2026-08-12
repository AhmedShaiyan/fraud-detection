"""
Synthetic card transaction producer.

Generates realistic card transactions and streams them to the `transactions`
Kafka topic, keyed by card_id (per-card ordering within a partition).

Event model: every transaction starts life as an AUTH. Approved AUTHs
(auth_code == "00") settle ~95% of the time after a real 2-48h delay;
declined AUTHs never settle. A small fraction (~0.5%) of SETTLEMENTs are
orphans that reference no real AUTH (force posts). Most settlements clear for
exactly the authorized amount; ~25% carry small FX/rounding drift and ~5% a
tip at restaurants/hotels (see settlement_drift). AUTH and SETTLEMENT events
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

Four independent random streams, so that turning one knob never silently
reshuffles another part of the simulation:
  1. entity      - fixed ENTITY_SEED, drives build_card_pool/build_merchant_pool
                    entirely (including the ids themselves). Every run, at any
                    --seed, reconstructs the identical card/merchant master
                    population, so card_id/merchant_id are stable join keys
                    across runs and a warehouse dimension built from them
                    accumulates history instead of a fresh set of strangers.
  2. simulation  - --seed (default: fresh entropy per run). Arrival times,
                    amounts, countries, fraud draws, settlement decisions.
  3. dirty       - fixed DIRTY_SEED, see --dirty-rate. Held separate so that
                    enabling dirty injection does not consume simulation draws
                    and therefore does not change which transactions get
                    generated: a --dirty-rate 0 run and a --dirty-rate 0.3 run
                    at the same --seed are the same underlying simulation.
  4. drift       - unseeded per run by default, see MERCHANT_DRIFT_RATE.

transaction_id stays uuid4 (not drawn from any of the four): every event is a
new event, so ids are deliberately unique per run even at a fixed --seed.

Data-quality defects (--dirty-rate) are injected at emission time, on a copy
of an already-valid AUTH, downstream of the generators - so a corrupted event
goes out on the wire while the settlement scheduled off its pristine original
is unaffected. See _dirty_stream.

Merchant drift (MERCHANT_DRIFT_RATE) mutates ~2% of merchants' name or MCC
once per run, against the canonical entity-seeded pool - the run-over-run
variation a Type-2 SCD merchant snapshot exists to capture.

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
    python producer.py --parquet-out ./out --count 500 --dirty-rate 0.02
    python producer.py --seed 7 --count 1000                        # reproducible simulation
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

TOPIC = "transactions"
BOOTSTRAP = "localhost:9092"

# --- Random streams (see module docstring) --------------------------------------

ENTITY_SEED = 42     # card/merchant master population; never varies
DIRTY_SEED = 1337    # which AUTHs get corrupted, and how

# The simulation stream. Every random draw in this module that is *not* about
# building entities, corrupting events, or drifting merchants goes through
# _SIM, so the whole simulation is reproducible from a single --seed while the
# other three streams stay independent of it. Module-level (rather than
# threaded through every signature) to keep the generator functions callable
# as plain functions, matching how they already read.
_SIM = random.Random()


def _entity_rng(entity_seed: int, stream: str) -> random.Random:
    """An independent entity sub-stream per pool. Seeding both pools with the
    plain entity_seed would draw their ids from the same position in the same
    sequence, so card_id and merchant_id come out sharing leading hex digits -
    harmless, but it reads like a bug. A string seed is hashed (sha512), so
    this stays deterministic across runs and processes."""
    return random.Random(f"{entity_seed}:{stream}")


def seed_simulation(seed: int | None = None) -> None:
    """Seed the simulation stream. None (the --seed default) draws fresh OS
    entropy, so an unseeded run is genuinely different every time; tests seed
    explicitly. Does not touch the entity/dirty/drift streams."""
    _SIM.seed(seed)


# --- Simulation tuning ----------------------------------------------------------

NEVER_SETTLE_RATE = 0.05       # of approved auths, fraction that never settle
ORPHAN_SETTLEMENT_RATE = 0.005 # independent chance per auth of an orphan settlement
# Settlement amount drift is a MIXTURE, not one uniform band. Most card
# settlements clear for exactly the authorized amount; drift is the exception,
# and it comes from two mechanisms that look nothing alike. Modelling it as a
# single uniform +/-20% made roughly half of all settlements breach the 10%
# reconciliation tolerance, i.e. an implied ~50% break rate - no real recon
# desk would tolerate that, and it drowned the genuinely interesting breaks.
SETTLEMENT_SMALL_DRIFT_RATE = 0.25    # FX conversion / rounding, either direction
SETTLEMENT_SMALL_DRIFT = 0.05         # +/- 5%, comfortably inside recon tolerance
SETTLEMENT_TIP_RANGE = (0.05, 0.25)   # gratuity, always upward
# Tips only happen where tipping happens. Restaurants and hotels are ~20% of
# settlements (merchants draw MCCs near-uniformly from the 10 in MCC), so a
# 25% tip rate at those MCCs works out to ~5% of settlements overall - which
# is what makes the global mixture land on ~70/25/5 without hard-coding it.
TIPPABLE_MCCS = ("5812", "7011")      # Restaurants, Hotels
SETTLEMENT_TIP_RATE_TIPPABLE = 0.25
SETTLEMENT_DELAY_HOURS = (2, 48)  # real auth->settlement delay range

HOME_COUNTRY_RATE = 0.97       # normal txns that stay in the card's home country
TRAVEL_TRIP_LENGTH = (3, 8)    # consecutive normal txns spent in one foreign country per trip
VELOCITY_BURST_GAP_SECONDS = (5, 30)  # spacing between consecutive events in a velocity burst
GEO_IMPOSSIBLE_SPEED_KMH = (2000, 20000)  # target implied speed a geo_impossible pair is built to hit

FLIGHT_SPEED_KMH = 800         # commercial-flight cruise speed, for travel-delay gating
TRAVEL_DELAY_JITTER = (1.0, 1.5)  # multiplier on pure flight time (boarding/customs/ground time)
GROUND_SPEED_KMH = 80          # ground travel speed ceiling for same-country card-present merchant hops

MERCHANT_DRIFT_RATE = 0.02     # per run, chance a merchant's name or mcc mutates

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

# Mirrors the valid_currencies dbt var (dbt_project.yml), which is what
# dbt/macros/transaction_validity.sql enforces in the warehouse.
VALID_CURRENCIES = set(CURRENCY_BY_COUNTRY.values())

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


def build_card_pool(n_cards: int = 500, entity_seed: int = ENTITY_SEED) -> list[dict]:
    """Each card gets a home country, a typical spend level, and preferred merchants.
    Card-level behavioral baselines are what make anomalies detectable.

    Built entirely from a *locally constructed* entity RNG, so this is a pure
    function of (entity_seed, n_cards): every run rebuilds the identical
    population, and calling it twice in one process returns the same cards.
    card_id therefore comes from that RNG rather than uuid4 - a uuid4 id would
    make every run a fresh set of strangers, and no warehouse dimension keyed
    on card_id could ever accumulate history. The first N cards of a larger
    pool are the same cards, so shrinking n_cards for a test is safe.

    current_country/travel_remaining/pending_country/next_available_at are
    mutable trip state (see _normal_txn_country): a fresh card is home-bound
    (current_country == home_country) with no pending flight.

    last_present_merchant/last_present_at track the card's most recent
    card-present (physical) transaction, for ground-speed-limited merchant
    selection (see _pick_reachable_merchant): a fresh card has no such
    history yet.
    """
    rng = _entity_rng(entity_seed, "cards")
    cards = []
    for _ in range(n_cards):
        home = rng.choices(list(COUNTRIES), weights=[w for _, _, w in COUNTRIES.values()])[0]
        cards.append({
            "card_id": f"card_{rng.getrandbits(48):012x}",
            "home_country": home,
            "avg_spend": round(rng.lognormvariate(3.5, 0.8), 2),  # median ~SGD 33
            "preferred_mccs": rng.sample(list(MCC), k=4),
            "current_country": home,
            "travel_remaining": 0,
            "pending_country": None,
            "next_available_at": None,
            "last_present_merchant": None,
            "last_present_at": None,
        })
    return cards


def build_merchant_pool(n_merchants: int = 300, entity_seed: int = ENTITY_SEED,
                         drift_rate: float = MERCHANT_DRIFT_RATE,
                         drift_seed: int | None = None) -> list[dict]:
    """Each merchant gets fixed lat/lon at creation (country reference point
    plus one-time jitter) - a merchant is a physical place, so every
    transaction at it should carry exactly the same coordinates.

    Same entity-RNG contract as build_card_pool: the pool is a pure function
    of (entity_seed, n_merchants), so merchant_id is a stable join key across
    runs. Merchant *attributes* are then drifted on top of that canonical pool
    by _apply_merchant_drift.
    """
    rng = _entity_rng(entity_seed, "merchants")
    entity_fake = Faker()
    entity_fake.seed_instance(entity_seed)  # instance-local, so drift's Faker can't perturb it

    merchants = []
    for _ in range(n_merchants):
        country = rng.choices(list(COUNTRIES), weights=[w for _, _, w in COUNTRIES.values()])[0]
        lat, lon, _ = COUNTRIES[country]
        merchants.append({
            "merchant_id": f"m_{rng.getrandbits(40):010x}",
            "merchant_name": entity_fake.company(),
            "mcc": rng.choice(list(MCC)),
            "country": country,
            "lat": round(lat + rng.uniform(-0.3, 0.3), 4),
            "lon": round(lon + rng.uniform(-0.3, 0.3), 4),
            "drifted": False,
        })
    _apply_merchant_drift(merchants, drift_rate, drift_seed)
    return merchants


def _apply_merchant_drift(merchants: list[dict], drift_rate: float,
                           drift_seed: int | None) -> int:
    """Mutate each merchant's name or mcc with probability drift_rate, once
    per run, in place. Returns how many drifted.

    This is the producer half of the Type-2 SCD story: merchants rebrand and
    get recategorized, so the merchant master a warehouse sees today is not
    the one it saw last week, and a snapshot has to version rather than
    overwrite. mcc_description is not drifted directly - base_txn derives it
    from MCC[merchant["mcc"]], so it follows the drifted mcc automatically.

    drift_seed=None (the default) draws fresh OS entropy, deliberately unlike
    every other stream here. The entity pool is reproducible by design, so two
    runs would otherwise emit an identical merchant master and the snapshot
    would never see a second version - the one thing it exists to capture.
    Tests pass an int.

    Drift is non-cumulative: each run mutates the canonical entity-seeded pool
    fresh, so run N+1's drift is applied to the original attributes rather
    than to run N's drifted ones. A merchant can therefore "revert" between
    runs. Acceptable at portfolio scale - the snapshot still versions
    correctly, it just sees a walk around the canonical values rather than a
    monotonic one; persisting drift state across runs would mean giving the
    producer a state file it otherwise does not need.
    """
    rng = random.Random(drift_seed)
    drift_fake = Faker()
    drift_fake.seed_instance(rng.getrandbits(32))  # reproducible iff drift_seed is

    drifted = 0
    for merchant in merchants:
        if rng.random() >= drift_rate:
            continue
        if rng.random() < 0.5:
            # Guarded: a "mutation" that lands on the same value would be an
            # invisible drift, and the snapshot would correctly record nothing.
            for _ in range(10):
                new_name = drift_fake.company()
                if new_name != merchant["merchant_name"]:
                    merchant["merchant_name"] = new_name
                    break
        else:
            merchant["mcc"] = rng.choice([m for m in MCC if m != merchant["mcc"]])
        merchant["drifted"] = True
        drifted += 1
    return drifted


# --- Transaction generation ---------------------------------------------------

def derive_entry_fields(channel: str) -> tuple[str, bool]:
    """POS entry mode + card-present flag, derived from channel."""
    if channel == "ONLINE":
        return "ECOMMERCE", False
    if channel == "CONTACTLESS":
        return "CONTACTLESS", True
    if channel == "ATM":
        return "CHIP", True
    return _SIM.choices(["CHIP", "SWIPE", "MANUAL"], weights=[80, 15, 5])[0], True


def derive_auth_code() -> str:
    return _SIM.choices(AUTH_CODES, weights=AUTH_CODE_WEIGHTS)[0]


def base_txn(card: dict, merchant: dict, amount: float, ts: datetime | None = None,
             channel: str | None = None, country: str | None = None) -> dict:
    country = country or merchant["country"]
    channel = channel or _SIM.choices(CHANNELS, weights=[45, 30, 10, 15])[0]
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
    return haversine_km(lat1, lon1, lat2, lon2) / FLIGHT_SPEED_KMH * _SIM.uniform(*TRAVEL_DELAY_JITTER)


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

    if _SIM.random() < HOME_COUNTRY_RATE:
        target, trip_length = card["home_country"], 0
    else:
        foreign = [c for c in COUNTRIES if c != card["home_country"]]
        target = _SIM.choice(foreign)
        trip_length = _SIM.randint(*TRAVEL_TRIP_LENGTH) - 1  # this txn's arrival is trip txn #1

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
        return _SIM.choice(eligible)

    elapsed_hours = max((event_time - last_at).total_seconds() / 3600, 0)
    max_reachable_km = elapsed_hours * GROUND_SPEED_KMH
    reachable = [m for m in eligible
                 if haversine_km(last_merchant["lat"], last_merchant["lon"], m["lat"], m["lon"])
                 <= max_reachable_km]
    return _SIM.choice(reachable) if reachable else last_merchant


def normal_txn(card: dict, merchants: list[dict], ts: datetime | None = None) -> list[dict]:
    event_time = ts or datetime.now(timezone.utc)
    country = _normal_txn_country(card, event_time)
    channel = _SIM.choices(CHANNELS, weights=[45, 30, 10, 15])[0]
    is_present = channel != "ONLINE"  # matches derive_entry_fields' own ONLINE -> card_present=False rule
    eligible = [m for m in merchants if m["country"] == country] or merchants

    if is_present:
        merchant = _pick_reachable_merchant(card, eligible, country, event_time)
        card["last_present_merchant"] = merchant
        card["last_present_at"] = event_time
    else:
        merchant = _SIM.choice(eligible)  # CNP: no location to be reachable from

    # spend near the card's baseline, log-normal noise
    amount = max(1.0, _SIM.lognormvariate(0, 0.6) * card["avg_spend"])
    return [base_txn(card, merchant, amount, event_time, channel=channel, country=country)]


def fraud_velocity(card: dict, merchants: list[dict], ts: datetime | None = None) -> list[dict]:
    """5-10 small-to-medium transactions on one card and merchant, spaced
    VELOCITY_BURST_GAP_SECONDS apart (up to ~4.5 min end-to-end for a 10-txn
    burst) so implied_speed_kmh-style elapsed-time features see a real,
    strictly-ascending timeline instead of a same-second pile-up. Classic
    card-testing / stolen-card cashout pattern."""
    txns = []
    current_ts = ts or datetime.now(timezone.utc)
    merchant = _SIM.choice(merchants)
    for i in range(_SIM.randint(5, 10)):
        if i > 0:
            current_ts += timedelta(seconds=_SIM.uniform(*VELOCITY_BURST_GAP_SECONDS))
        amount = _SIM.uniform(1, 3) * card["avg_spend"]
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
    c1, c2 = _SIM.sample(list(COUNTRIES), 2)
    dist_km = haversine_km(*COUNTRIES[c1][:2], *COUNTRIES[c2][:2])
    target_speed_kmh = _SIM.uniform(*GEO_IMPOSSIBLE_SPEED_KMH)
    gap_hours = dist_km / target_speed_kmh

    txns = []
    for i, country in enumerate((c1, c2)):
        event_time = base_ts if i == 0 else base_ts + timedelta(hours=gap_hours)
        eligible = [m for m in merchants if m["country"] == country] or merchants
        merchant = _SIM.choice(eligible)
        amount = _SIM.uniform(0.5, 4) * card["avg_spend"]
        t = base_txn(card, merchant, amount, event_time, channel="POS", country=country)
        t["is_fraud"], t["fraud_type"] = 1, "geo_impossible"
        txns.append(t)
    return txns


def fraud_amount_anomaly(card: dict, merchants: list[dict], ts: datetime | None = None) -> list[dict]:
    """Single transaction 10-50x the card's typical spend, often high-risk MCC."""
    merchant = _SIM.choice([m for m in merchants if m["mcc"] in ("5967", "5732", "4511")]
                             or merchants)
    amount = _SIM.uniform(10, 50) * card["avg_spend"]
    t = base_txn(card, merchant, amount, ts, channel="ONLINE")
    t["is_fraud"], t["fraud_type"] = 1, "amount_anomaly"
    return [t]


FRAUD_GENERATORS = [fraud_velocity, fraud_geo_impossible, fraud_amount_anomaly]


def generate_auth_batch(card: dict, merchants: list[dict], fraud_rate: float,
                         ts: datetime | None = None) -> list[dict]:
    """One card's worth of AUTH events for this turn: fraud with probability
    fraud_rate (via one of the three generators), else a normal transaction."""
    if _SIM.random() < fraud_rate:
        return _SIM.choice(FRAUD_GENERATORS)(card, merchants, ts)
    return normal_txn(card, merchants, ts)


# --- Settlement generation ------------------------------------------------------

def settlement_drift(auth: dict) -> float:
    """Fractional difference between the settled and authorized amount, drawn
    from a three-way mixture (see the constants above):

      ~70%  exact match      - drift 0.0, the settlement clears for exactly
                               what was authorized
      ~25%  small drift      - uniform +/- SETTLEMENT_SMALL_DRIFT, FX
                               conversion and rounding, symmetric because
                               either side can round in your favour
      ~5%   tip              - uniform over SETTLEMENT_TIP_RANGE, always
                               UPWARD (a gratuity only ever increases the
                               amount) and only at TIPPABLE_MCCS

    The exact/small split is the same everywhere; only the tip branch is
    MCC-conditional, so the headline 70/25/5 is an emergent average over the
    MCC mix rather than a hard-coded global constant.
    """
    tip_rate = SETTLEMENT_TIP_RATE_TIPPABLE if auth["mcc"] in TIPPABLE_MCCS else 0.0
    roll = _SIM.random()
    if roll < tip_rate:
        return _SIM.uniform(*SETTLEMENT_TIP_RANGE)
    if roll < tip_rate + SETTLEMENT_SMALL_DRIFT_RATE:
        return _SIM.uniform(-SETTLEMENT_SMALL_DRIFT, SETTLEMENT_SMALL_DRIFT)
    return 0.0


def decide_settlement(auth: dict, delay_hours_range: tuple[float, float] = SETTLEMENT_DELAY_HOURS
                       ) -> tuple[dict | None, float]:
    """Decide if/when/how an AUTH settles.

    Declined auths (auth_code != "00") never settle. Approved auths settle
    ~95% of the time (NEVER_SETTLE_RATE), after a delay drawn uniformly from
    delay_hours_range real hours, with the amount drifted per the mixture in
    settlement_drift (most settlements clear exactly; the rest carry FX/
    rounding noise or a tip). Returns (settlement_dict_or_None, delay_hours) -
    delay_hours is 0 when there is no settlement.
    """
    if auth["auth_code"] != "00":
        return None, 0
    if _SIM.random() < NEVER_SETTLE_RATE:
        return None, 0

    delay_hours = _SIM.uniform(*delay_hours_range)
    auth_time = datetime.fromisoformat(auth["event_time"])
    settle_time = auth_time + timedelta(hours=delay_hours)
    amount = round(auth["amount"] * (1 + settlement_drift(auth)), 2)

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
    card = _SIM.choice(cards)
    merchant = _SIM.choice(merchants)
    amount = max(1.0, _SIM.lognormvariate(0, 0.6) * card["avg_spend"])

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


# --- Data-quality defect injection ------------------------------------------------

# A timestamp that looks like one but parses as neither ISO-8601 in Python nor
# a timestamp in Spark (month 13, day 45, hour 99) - `cast(event_time as
# timestamp)` returns NULL for it, which is exactly what the warehouse-side
# validity check keys on (dbt/macros/transaction_validity.sql).
UNPARSEABLE_EVENT_TIME = "2026-13-45 99:99:99"


def _parses_as_timestamp(value) -> bool:
    if not isinstance(value, str):
        return False
    try:
        datetime.fromisoformat(value)
        return True
    except ValueError:
        return False


# What "malformed" means, as predicates. Kept alongside the mutators below so
# a defect can be *detected* as well as applied - tests assert exactly one
# holds per dirty event, and the run loop counts dirty events without adding a
# field to EVENT_FIELDS. Deliberately parallel to the reason list in
# dbt/macros/transaction_validity.sql, which is the warehouse's own copy of
# this definition.
DIRTY_DEFECT_CHECKS = {
    "NULL_TRANSACTION_ID": lambda e: e["transaction_id"] is None,
    "NULL_CARD_ID": lambda e: e["card_id"] is None,
    "NONPOSITIVE_AMOUNT": lambda e: e["amount"] is None or e["amount"] <= 0,
    "UNKNOWN_CURRENCY": lambda e: e["currency"] not in VALID_CURRENCIES,
    "UNPARSEABLE_EVENT_TIME": lambda e: not _parses_as_timestamp(e["event_time"]),
}


def _corrupt_amount(event: dict, rng: random.Random) -> None:
    event["amount"] = 0.0 if rng.random() < 0.5 else -abs(event["amount"])


DIRTY_DEFECT_MUTATORS = {
    "NULL_TRANSACTION_ID": lambda e, rng: e.update(transaction_id=None),
    "NULL_CARD_ID": lambda e, rng: e.update(card_id=None),
    "NONPOSITIVE_AMOUNT": _corrupt_amount,
    "UNKNOWN_CURRENCY": lambda e, rng: e.update(currency="XXX"),
    "UNPARSEABLE_EVENT_TIME": lambda e, rng: e.update(event_time=UNPARSEABLE_EVENT_TIME),
}


def defects(event: dict) -> set[str]:
    """Which defects an event exhibits. Empty set = clean."""
    return {name for name, check in DIRTY_DEFECT_CHECKS.items() if check(event)}


def corrupt_auth(event: dict, rng: random.Random) -> dict:
    """Return a corrupted *copy* of an AUTH, carrying exactly one defect drawn
    evenly from DIRTY_DEFECT_MUTATORS. The copy matters: _event_stream yields
    an auth before scheduling its settlement off the same dict, so corrupting
    in place would propagate the defect into a settlement that, in a real
    system, was derived from the authorization the issuer actually approved."""
    dirty = dict(event)
    DIRTY_DEFECT_MUTATORS[rng.choice(sorted(DIRTY_DEFECT_MUTATORS))](dirty, rng)
    return dirty


def _dirty_stream(stream, dirty_rate: float, rng: random.Random | None = None):
    """Wrap an event stream, corrupting AUTHs at dirty_rate (per AUTH, not per
    event - SETTLEMENTs are never corrupted).

    Draws from its own RNG so that enabling --dirty-rate consumes no
    simulation draws: at a fixed --seed the clean and dirty runs are the same
    underlying simulation, differing only in the corrupted fields.

    Note the deliberate downstream consequence: an AUTH whose transaction_id
    is nulled here still has its settlement scheduled against the original id,
    so that settlement arrives referencing an auth that appears nowhere in the
    stream and reconciles as ORPHAN_SETTLEMENT. That is faithful - the auth
    was corrupted on the wire, it did not un-happen - but it means a high
    --dirty-rate inflates the apparent force-post rate.
    """
    rng = rng or random.Random(DIRTY_SEED)
    for event, due_at, kind in stream:
        if kind == "auth" and dirty_rate > 0 and rng.random() < dirty_rate:
            yield corrupt_auth(event, rng), due_at, kind
        else:
            yield event, due_at, kind


# --- Unified event-driven scheduling --------------------------------------------

def _sample_interarrival_seconds(txns_per_day_per_card: float) -> float:
    """Exponential inter-arrival time for a Poisson arrival process at the
    given per-card daily rate."""
    rate_per_second = txns_per_day_per_card / 86400
    return _SIM.expovariate(rate_per_second)


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

            if _SIM.random() < ORPHAN_SETTLEMENT_RATE:
                orphan = orphan_settlement(cards, merchants, ts=a_time)
                orphan_at = a_time + timedelta(minutes=_SIM.uniform(0, 5))
                heapq.heappush(heap, (orphan_at, next(seq), "orphan", orphan))

        next_due = last_event_time + timedelta(seconds=_sample_interarrival_seconds(txns_per_day_per_card))
        if card["next_available_at"] is not None and card["next_available_at"] > next_due:
            next_due = card["next_available_at"]
        heapq.heappush(heap, (next_due, next(seq), "txn", card))


# --- Kafka plumbing -----------------------------------------------------------

def delivery_report(err, msg):
    if err is not None:
        print(f"DELIVERY FAILED: {err}")


def _message_key(event: dict) -> bytes | None:
    """Partition key: card_id, for per-card ordering. None for a dirty event
    whose card_id was nulled - there is no card to order it by, so it takes
    the default (round-robin) partition assignment rather than crashing the
    producer or, worse, being silently dropped before the warehouse can
    quarantine it."""
    card_id = event["card_id"]
    return card_id.encode() if card_id is not None else None


def emit(producer: Producer, event: dict) -> None:
    producer.produce(
        TOPIC,
        key=_message_key(event),
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
    stream = _dirty_stream(
        _event_stream(cards, merchants, args.fraud_rate, args.txns_per_day_per_card,
                       SETTLEMENT_DELAY_HOURS, start_time, heap=heap),
        args.dirty_rate,
    )

    sent = auth_sent = fraud_sent = settled_sent = orphan_sent = dirty_sent = 0
    prev_synthetic = start_time
    interrupted = False

    print(f"Producing to '{TOPIC}': {args.txns_per_day_per_card} txns/day/card x {len(cards)} cards, "
          f"{args.time_compression}x time compression, {args.dirty_rate:.1%} dirty AUTHs, "
          f"{sum(1 for m in merchants if m['drifted'])} merchants drifted this run. Ctrl+C to stop.")
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
                if args.dirty_rate and defects(event):
                    dirty_sent += 1
            elif kind == "settlement":
                settled_sent += 1
            elif kind == "orphan":
                orphan_sent += 1

            producer.poll(0)
            if sent % 100 == 0:
                print(f"sent={sent}  fraud={fraud_sent}  settled={settled_sent}  "
                      f"orphans={orphan_sent}  dirty={dirty_sent}  "
                      f"synthetic_time={due_at.isoformat()}  "
                      f"fraud_rate={fraud_sent / max(sent, 1):.2%}")
    except KeyboardInterrupt:
        interrupted = True
    finally:
        producer.flush()
        print(f"\nDone. sent={sent}, fraud={fraud_sent}, settled={settled_sent}, "
              f"orphans={orphan_sent}, dirty={dirty_sent}")
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
                     settlement_delay_hours: tuple[float, float] = SETTLEMENT_DELAY_HOURS,
                     dirty_rate: float = 0.0) -> list[dict]:
    """Generate n events via the unified synthetic-clock event stream. The
    start time is anchored so the run's span (estimated from the aggregate
    per-card arrival rate) ends near generation wall time - this is a
    stochastic Poisson process, so the last event lands approximately, not
    exactly, at "now"."""
    aggregate_rate_per_second = len(cards) * txns_per_day_per_card / 86400
    expected_span_seconds = n / aggregate_rate_per_second
    start_time = datetime.now(timezone.utc) - timedelta(seconds=expected_span_seconds)

    stream = _dirty_stream(
        _event_stream(cards, merchants, fraud_rate, txns_per_day_per_card,
                       settlement_delay_hours, start_time),
        dirty_rate,
    )
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
    events = generate_events(args.count, cards, merchants, args.fraud_rate,
                              args.txns_per_day_per_card, dirty_rate=args.dirty_rate)
    path = write_parquet(events, Path(args.parquet_out))

    fraud = sum(e["is_fraud"] for e in events)
    settlements = sum(1 for e in events if e["event_type"] == "SETTLEMENT")
    dirty = sum(1 for e in events if defects(e))
    drifted = sum(1 for m in merchants if m["drifted"])
    print(f"Wrote {len(events)} events ({fraud} fraud, {settlements} settlements, "
          f"{dirty} dirty) to {path}; {drifted} merchants drifted this run")


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
    parser.add_argument("--dirty-rate", type=float, default=0.0,
                         help="fraction of AUTH events emitted malformed (null transaction_id / "
                              "nonpositive amount / unknown currency / unparseable event_time / "
                              "null card_id). Per AUTH, not per event; SETTLEMENTs are never "
                              "corrupted. Exercises the Silver quarantine path.")
    parser.add_argument("--seed", type=int, default=None,
                         help="seed the simulation stream (arrival times, amounts, fraud draws) for a "
                              "reproducible run. The card/merchant master population is always "
                              "reproducible regardless, via the fixed ENTITY_SEED.")
    args = parser.parse_args()

    seed_simulation(args.seed)

    if args.parquet_out:
        if args.count <= 0:
            parser.error("--parquet-out requires --count > 0")
        run_parquet(args)
    else:
        run_kafka(args)


if __name__ == "__main__":
    main()
