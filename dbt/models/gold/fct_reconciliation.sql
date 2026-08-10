{{
  config(
    materialized='table'
  )
}}

-- materialized='table' (full rebuild every run), not incremental: recon_status
-- is partly a function of current_timestamp() (an unsettled auth is
-- PENDING_SETTLEMENT vs AUTH_NO_SETTLEMENT depending on how much time has
-- elapsed since it happened), so a row's correct status changes between runs
-- even though the underlying auth/settlement rows never do. An incremental
-- append would freeze old rows at whatever status they had the day they were
-- inserted. At production scale the fix isn't "materialize everything
-- forever" but an incremental model with a lookback window: only rebuild
-- auths from the last N days, since anything older than the maturity window
-- is final and never needs to be revisited.

with auths as (

    select
        transaction_id,
        card_id_hash,
        event_time,
        amount,
        auth_code
    from {{ ref('slv_authorizations') }}

),

settlements as (

    select
        transaction_id,
        auth_transaction_id,
        card_id_hash,
        event_time,
        amount
    from {{ ref('slv_settlements') }}

),

joined as (

    -- Grain: one row per AUTH transaction plus one row per orphan
    -- SETTLEMENT (a settlement whose auth_transaction_id matches no real
    -- AUTH). The full outer join on transaction_id = auth_transaction_id
    -- is exactly that grain: every AUTH gets a row (matched to 0 or 1
    -- settlements), and every SETTLEMENT with no matching AUTH gets its own
    -- row via the right-side-only outer join results.
    select
        auths.transaction_id            as auth_txn_id_from_auth,
        settlements.auth_transaction_id as auth_txn_id_from_settlement,
        settlements.transaction_id      as settlement_transaction_id,
        auths.card_id_hash              as auth_card_id_hash,
        settlements.card_id_hash        as settlement_card_id_hash,
        auths.amount                    as auth_amount,
        settlements.amount              as settlement_amount,
        auths.event_time                as auth_time,
        settlements.event_time          as settlement_time,
        auths.auth_code                 as auth_code
    from auths
    full outer join settlements
        on auths.transaction_id = settlements.auth_transaction_id

),

enriched as (

    select
        coalesce(auth_txn_id_from_auth, auth_txn_id_from_settlement) as auth_transaction_id,
        settlement_transaction_id,
        coalesce(auth_card_id_hash, settlement_card_id_hash) as card_id_hash,
        auth_amount,
        settlement_amount,
        (settlement_amount - auth_amount) as amount_diff,
        (settlement_amount - auth_amount) / nullif(auth_amount, 0) as amount_diff_pct,
        auth_time,
        settlement_time,
        timestampdiff(minute, auth_time, settlement_time) as settlement_lag_minutes,
        auth_code,
        (auth_txn_id_from_auth is not null) as has_auth,
        (settlement_transaction_id is not null) as has_settlement,
        (
            auth_time is not null
            and auth_time <= current_timestamp() - interval {{ var('recon_maturity_minutes', 30) }} minutes
        ) as is_matured
    from joined

),

classified as (

    select
        *,
        -- AMOUNT_MISMATCH break conditions (independent; either or both can fire):
        --   cond_pct_exceeded     - drift is more than $1 AND more than recon_amount_tolerance_pct of the auth amount
        --   cond_ceiling_exceeded - drift exceeds the absolute dollar-exposure ceiling, regardless of percentage
        (abs(amount_diff) > 1.00 and abs(amount_diff_pct) > {{ var('recon_amount_tolerance_pct', 0.10) }}) as cond_pct_exceeded,
        (abs(amount_diff) > {{ var('recon_amount_ceiling_abs', 100.00) }}) as cond_ceiling_exceeded
    from enriched

),

statused as (

    select
        *,
        case
            when has_auth and auth_code <> '00' then 'AUTH_DECLINED'
            when has_auth and has_settlement and (cond_pct_exceeded or cond_ceiling_exceeded) then 'AMOUNT_MISMATCH'
            when has_auth and has_settlement then 'MATCHED'
            when has_auth and not has_settlement and is_matured then 'AUTH_NO_SETTLEMENT'
            when has_auth and not has_settlement and not is_matured then 'PENDING_SETTLEMENT'
            when not has_auth and has_settlement then 'ORPHAN_SETTLEMENT'
        end as recon_status
    from classified

)

select

    auth_transaction_id,
    settlement_transaction_id,
    card_id_hash,
    auth_amount,
    settlement_amount,
    amount_diff,
    amount_diff_pct,
    auth_time,
    settlement_time,
    settlement_lag_minutes,
    recon_status,
    recon_status in ('AMOUNT_MISMATCH', 'AUTH_NO_SETTLEMENT', 'ORPHAN_SETTLEMENT') as is_break,

    case
        when recon_status = 'AMOUNT_MISMATCH' and cond_pct_exceeded and cond_ceiling_exceeded then 'PCT_AND_CEILING'
        when recon_status = 'AMOUNT_MISMATCH' and cond_pct_exceeded then 'PCT_EXCEEDED'
        when recon_status = 'AMOUNT_MISMATCH' and cond_ceiling_exceeded then 'CEILING_EXCEEDED'
        when recon_status = 'AUTH_NO_SETTLEMENT' then 'SETTLEMENT_TIMEOUT'
        when recon_status = 'ORPHAN_SETTLEMENT' then 'NO_MATCHING_AUTH'
        else null
    end as break_reason,

    is_matured

from statused
