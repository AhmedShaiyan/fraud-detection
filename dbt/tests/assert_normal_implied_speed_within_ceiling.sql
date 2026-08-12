-- Non-fraud implied_speed_kmh must respect a physically plausible ceiling:
-- card-present pairs are ground-speed-limited at generation time (see
-- producer.py's GROUND_SPEED_KMH merchant re-pick), and CNP rows never
-- contribute to the calculation at all (see model header comment). Fails
-- if any non-fraud row's implied speed exceeds 900 km/h - well above
-- commercial ground speed, comfortably below the flight-speed ceiling
-- legitimate cross-country travel is gated at.
--
-- Rows measured against a FRAUD predecessor are excluded. implied_speed_kmh
-- is computed against the nearest prior card-present event, and the
-- geo_impossible generator deliberately teleports the card without moving
-- the cardholder's tracked position - correctly so, since a stolen card
-- being used abroad doesn't relocate the legitimate holder. The next genuine
-- transaction therefore measures its speed from wherever the fraud left off
-- and inherits an impossible value while carrying fraud_type = null. That is
-- a real false-positive source worth keeping in the feature (see the model
-- header), not a generation bug, so this test scopes itself to legitimate
-- pairs: both endpoints non-fraud.
--
-- Pairs that straddle two producer batches are also excluded (_source_file
-- equality). A --parquet-out run is a self-contained simulation: entities are
-- stable across runs by design (fixed ENTITY_SEED), but each run re-simulates
-- every card's trajectory from a fresh home-country state, so two batches
-- describe the same card taking two independent journeys. Interleaved by
-- event_time in the warehouse those journeys are incoherent by construction,
-- and no physics claim was ever made about them. Kafka mode has no such
-- caveat - it is one continuous simulation on a single clock, so cross-batch
-- coherence does hold there and this scoping is a no-op for it.
--
-- The lag mirrors the feature model's own expression exactly - same source,
-- same partition, same ordering, same ignore-nulls-over-card-present form -
-- so the predecessor identified here is the one the speed was actually
-- computed against. Two details that matter:
--   * it keys on is_fraud, not fraud_type. fraud_type is null for every
--     legitimate row, so `ignore nulls` over it would skip all of them and
--     reach back to some older fraud row, silently excluding rows whose real
--     predecessor was perfectly normal. is_fraud is 0/1 and never null, and
--     _source_file is likewise never null, so both lags land on the same row.
--   * same-second ties are ordered arbitrarily by Spark in both places (see
--     the model header), so on a tied pair this can in principle name a
--     different predecessor than the feature did.

with with_predecessor as (

    select
        transaction_id,
        _source_file,
        lag(case when card_present then is_fraud end) ignore nulls over (
            partition by card_id_hash order by unix_timestamp(event_time)
        ) as prev_card_present_is_fraud,
        lag(case when card_present then _source_file end) ignore nulls over (
            partition by card_id_hash order by unix_timestamp(event_time)
        ) as prev_card_present_source_file
    from {{ ref('slv_authorizations') }}

)

select
    features.transaction_id,
    features.card_id_hash,
    features.event_time,
    features.implied_speed_kmh,
    predecessor._source_file,
    predecessor.prev_card_present_source_file
from {{ ref('fct_card_velocity_features') }} as features
join with_predecessor as predecessor
    on predecessor.transaction_id = features.transaction_id
where features.fraud_type is null
  and features.implied_speed_kmh > 900
  and coalesce(predecessor.prev_card_present_is_fraud, 0) = 0
  and predecessor._source_file = predecessor.prev_card_present_source_file
