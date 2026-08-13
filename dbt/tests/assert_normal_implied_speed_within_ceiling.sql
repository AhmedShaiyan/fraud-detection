-- Non-fraud implied_speed_kmh must stay under a physically plausible ceiling
-- (900 km/h - above ground speed, below the flight-speed gate): card-present
-- pairs are ground-speed-limited at generation time (producer.py's
-- GROUND_SPEED_KMH), and CNP rows never contribute to the calculation.
--
-- Excludes rows measured against a FRAUD predecessor: geo_impossible
-- teleports the card without moving the cardholder, so the next genuine
-- transaction inherits an impossible speed from wherever the fraud left off
-- - a real, deliberately-kept false-positive source (see model header), not
-- a bug, so this test scopes to legitimate pairs only.
--
-- Excludes pairs straddling two producer batches (_source_file mismatch):
-- each --parquet-out run re-simulates every card from a fresh state, so two
-- batches are independent journeys with no physics claim between them.
-- Kafka mode is one continuous simulation, so this is a no-op there.
--
-- The lag mirrors the feature model's expression exactly (same source,
-- partition, ordering, ignore-nulls-over-card-present) so it names the same
-- predecessor the feature was computed against. Keys on is_fraud, not
-- fraud_type, since fraud_type is null for every legitimate row and would
-- make `ignore nulls` skip past them. Same-second ties can in principle name
-- a different predecessor than the feature did (see model header).

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
