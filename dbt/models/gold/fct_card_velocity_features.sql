{{ config(materialized='table') }}

-- Full rebuild every run: window aggregates need a card's full ordered
-- history; incremental would leave rows stale after a late/backfilled auth.
-- Bounded lookback per card_id_hash would fix this at scale.
--
-- Training-serving parity: offline feature source only - a real-time scorer
-- needs its own live cache or point-in-time recompute.
--
-- Same-second ties: RANGE-frame features (txn_count_1h/24h, amount_sum_24h,
-- has_history) see same-second peers as history; distinct_countries_24h and
-- the LAG-based recency features don't. has_history uses the RANGE-count
-- technique, not a LAG NULL check, to stay consistent with the counts.
--
-- implied_speed_kmh: null for card-not-present rows, computed against the
-- nearest prior card-present row via `lag ... ignore nulls`. A legit txn
-- right after fraud inherits an anomalous speed by design (see
-- tests/assert_normal_implied_speed_within_ceiling.sql).

with auths as (

    select
        transaction_id,
        card_id_hash,
        event_time,
        unix_timestamp(event_time) as event_time_unix,
        amount,
        country,
        lat,
        lon,
        channel,
        pos_entry_mode,
        card_present,
        auth_code,
        mcc,
        is_fraud,
        fraud_type
    from {{ ref('slv_authorizations') }}

),

windowed as (

    select
        *,
        count(*) over (
            partition by card_id_hash order by event_time_unix
            range between 3600 preceding and current row
        ) as txn_count_1h_incl,
        count(*) over (
            partition by card_id_hash order by event_time_unix
            range between 86400 preceding and current row
        ) as txn_count_24h_incl,
        sum(amount) over (
            partition by card_id_hash order by event_time_unix
            range between 86400 preceding and current row
        ) as amount_sum_24h_incl,
        count(*) over (
            partition by card_id_hash order by event_time_unix
            range between unbounded preceding and current row
        ) as historical_txn_count_incl,
        -- Stops 1 second short of current row, unlike the inclusive counts above -
        -- excludes same-second peers entirely, not just this row.
        size(collect_set(country) over (
            partition by card_id_hash order by event_time_unix
            range between 86400 preceding and 1 preceding
        )) as distinct_countries_24h,
        lag(event_time_unix) over (partition by card_id_hash order by event_time_unix) as prev_event_time_unix,
        -- Nearest PRIOR card-present row's time/lat/lon, skipping CNP rows in between.
        lag(case when card_present then event_time_unix end) ignore nulls over (
            partition by card_id_hash order by event_time_unix
        ) as prev_present_event_time_unix,
        lag(case when card_present then lat end) ignore nulls over (
            partition by card_id_hash order by event_time_unix
        ) as prev_present_lat,
        lag(case when card_present then lon end) ignore nulls over (
            partition by card_id_hash order by event_time_unix
        ) as prev_present_lon
    from auths

),

features as (

    select
        transaction_id,
        card_id_hash,
        event_time,
        amount,
        (txn_count_1h_incl - 1) as txn_count_1h,
        (txn_count_24h_incl - 1) as txn_count_24h,
        (amount_sum_24h_incl - amount) as amount_sum_24h,
        case when (txn_count_24h_incl - 1) > 0
            then (amount_sum_24h_incl - amount) / (txn_count_24h_incl - 1)
        end as amount_avg_24h,
        distinct_countries_24h,
        case when prev_event_time_unix is not null
            then (event_time_unix - prev_event_time_unix) / 60.0
        end as minutes_since_last_txn,
        -- Card-present rows only, on both ends (see header comment).
        case when card_present and prev_present_event_time_unix is not null
            then {{ haversine_km('prev_present_lat', 'prev_present_lon', 'lat', 'lon') }}
                 / nullif((event_time_unix - prev_present_event_time_unix) / 3600.0, 0)
        end as implied_speed_kmh,
        card_present as is_card_present,
        (channel = 'ONLINE') as is_online,
        (mcc in ('5967', '5732', '4511')) as is_high_risk_mcc,
        (auth_code <> '00') as was_declined,
        -- Same inclusive-RANGE-minus-self technique as txn_count_1h/24h, unbounded.
        (historical_txn_count_incl - 1) > 0 as has_history,
        is_fraud,
        fraud_type
    from windowed

)

select
    transaction_id,
    card_id_hash,
    event_time,
    amount,
    txn_count_1h,
    txn_count_24h,
    amount_sum_24h,
    amount_avg_24h,
    case when amount_avg_24h is not null
        then amount / nullif(amount_avg_24h, 0)
    end as amount_vs_avg_24h_ratio,
    minutes_since_last_txn,
    distinct_countries_24h,
    implied_speed_kmh,
    is_card_present,
    is_online,
    is_high_risk_mcc,
    was_declined,
    has_history,

    -- LEAKAGE WARNING: ground-truth labels, for evaluation only - never a model input.
    is_fraud,
    fraud_type

from features
