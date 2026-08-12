-- AUTH-side events only.
--
-- Rows failing any check in macros/transaction_validity.sql are excluded here
-- and captured by slv_quarantine, which filters on the exact negation of the
-- same macro call - so this model never defines "valid" for itself, and a bad
-- row is held for inspection rather than dropped.

with source as (

    select *
    from {{ source('bronze', 'transactions_raw') }}
    where event_type = 'AUTH'
    and {{ transaction_is_valid() }}

    {% if is_incremental() %}
    and _ingested_at > (select max(_ingested_at) from {{ this }})
    {% endif %}

)

select
    transaction_id,
    event_type,
    -- try_cast, not cast: identical to cast for rows that reach here (the
    -- validity filter already guarantees this parses), but it cannot raise
    -- under ANSI mode if the optimizer evaluates the projection before the
    -- filter.
    try_cast(event_time as timestamp) as event_time,
    sha2(card_id, 256) as card_id_hash,
    merchant_id,
    merchant_name,
    mcc,
    mcc_description,
    cast(amount as decimal(12, 2)) as amount,
    currency,
    country,
    lat,
    lon,
    channel,
    pos_entry_mode,
    card_present,
    auth_code,
    _ingested_at,
    _source_file,

    -- LEAKAGE WARNING: is_fraud/fraud_type are ground-truth labels
    is_fraud,
    fraud_type

from source
