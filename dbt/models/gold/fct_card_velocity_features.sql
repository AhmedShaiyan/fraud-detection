{{ config(materialized='table') }}

-- materialized='table': full rebuild every run. Window aggregates depend on
-- a card's entire ordered history, and at this project's data volume a full
-- rebuild is cheap and removes any risk of a late-arriving/out-of-order
-- backfilled auth silently leaving downstream rows' point-in-time features
-- stale under an incremental/merge strategy. At production scale:
-- incremental with a bounded lookback window per card_id_hash, same
-- tradeoff as fct_reconciliation.
--
-- Training-serving parity: this table is the offline training feature
-- source only. A real-time scoring endpoint does not have a ready-made
-- trailing window for a brand-new incoming transaction; serving needs
-- either a live per-card feature cache (e.g. Redis, updated by the stream)
-- or point-in-time recomputation at request time. Training on these Gold
-- features and serving off a differently-computed online store is a
-- training-serving skew risk that has to be resolved explicitly if this
-- project reaches a serving stage.
--
-- Same-second tie caveat: event_time ties (same card, same second) are
-- ordered arbitrarily by Spark within the window. RANGE frames whose upper
-- bound is CURRENT ROW treat same-second peers as mutually visible to each
-- other (SQL RANGE semantics include every row tied on the ORDER BY value,
-- not just ones physically earlier) - so txn_count_1h/24h, amount_sum/avg_24h
-- and has_history (all built as an inclusive RANGE-to-CURRENT-ROW count minus
-- exactly one unit for self) correctly see same-second peers as concurrent
-- history. distinct_countries_24h and the LAG-based recency features
-- (minutes_since_last_txn, implied_speed_kmh) instead use a frame/offset
-- that stops strictly before the current row's value, so same-second peers
-- are invisible to each other there (a same-second burst won't register as
-- a new country or a nonzero speed until the next second's row). has_history
-- deliberately uses the RANGE-count technique rather than a LAG(...) IS NOT
-- NULL check for this reason: LAG only sees the one physically-preceding
-- row, so the physically-first row in a same-second burst would otherwise
-- report has_history=false while its RANGE-based txn_count_1h/24h already
-- count the other rows in that same burst - an inconsistency that would
-- break the "has_history=false implies null aggregates" invariant.
--
-- Card-present-only implied_speed_kmh: a card-not-present (online) auth's
-- merchant coordinates don't locate the cardholder at all - the cardholder
-- could be anywhere, so measuring distance/time to or from a CNP row
-- produces a physically meaningless number, mirroring how production
-- card-network geo-velocity checks only compare physically-present
-- transactions. implied_speed_kmh is therefore null for CNP rows and is
-- computed against the nearest PRIOR card-present row (lag ... ignore
-- nulls), not simply the immediately-preceding row of any kind.
--
-- A legitimate transaction following a fraudulent one inherits an anomalous
-- implied_speed_kmh, because the fraud moved the card but not the cardholder
-- - a realistic false-positive source, retained deliberately rather than
-- engineered away (see tests/assert_normal_implied_speed_within_ceiling.sql).

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
        -- "prior window": stops 1 second short of current row's value, so
        -- (unlike the inclusive-then-subtract-self counts above) this
        -- excludes same-second peers entirely, not just this one row.
        size(collect_set(country) over (
            partition by card_id_hash order by event_time_unix
            range between 86400 preceding and 1 preceding
        )) as distinct_countries_24h,
        lag(event_time_unix) over (partition by card_id_hash order by event_time_unix) as prev_event_time_unix,
        -- Nearest PRIOR card-present row's time/lat/lon, skipping over any
        -- CNP rows in between (see header comment) - not simply the
        -- immediately-preceding row like prev_event_time_unix above.
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
        -- Card-present rows only, on both ends: null for CNP rows
        -- themselves and for any row with no prior card-present event to
        -- compare against (see header comment).
        case when card_present and prev_present_event_time_unix is not null
            then {{ haversine_km('prev_present_lat', 'prev_present_lon', 'lat', 'lon') }}
                 / nullif((event_time_unix - prev_present_event_time_unix) / 3600.0, 0)
        end as implied_speed_kmh,
        card_present as is_card_present,
        (channel = 'ONLINE') as is_online,
        (mcc in ('5967', '5732', '4511')) as is_high_risk_mcc,
        (auth_code <> '00') as was_declined,
        -- Same inclusive-RANGE-minus-self technique as txn_count_1h/24h,
        -- just unbounded, so it stays consistent with them under
        -- same-second ties (see header comment) and guarantees
        -- has_history=false implies zero rows in every trailing window,
        -- including the ones amount_avg_24h/txn_count_24h are built from.
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

    -- LEAKAGE WARNING: is_fraud/fraud_type are ground-truth labels, carried
    -- through for evaluation/join-back only. Never use as model input
    -- features - the training notebook must exclude these two columns.
    is_fraud,
    fraud_type

from features
