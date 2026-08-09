-- SETTLEMENT-side events only. lat/lon/channel/pos_entry_mode/card_present/
-- auth_code are excluded: producer.py's decide_settlement/orphan_settlement
-- leave them null (those fields only make sense at the point of authorization).

with source as (

    select *
    from {{ source('bronze', 'transactions_raw') }}
    where event_type = 'SETTLEMENT'

    {% if is_incremental() %}
    and _ingested_at > (select max(_ingested_at) from {{ this }})
    {% endif %}

)

select
    transaction_id,
    event_type,
    cast(event_time as timestamp) as event_time,
    auth_transaction_id,
    sha2(card_id, 256) as card_id_hash,
    merchant_id,
    merchant_name,
    mcc,
    mcc_description,
    cast(amount as decimal(12, 2)) as amount,
    currency,
    country,
    _ingested_at,
    _source_file,

    -- LEAKAGE WARNING: is_fraud/fraud_type are ground-truth labels, kept
    -- here only for offline evaluation. Never join these into a feature
    -- table used at inference time.
    is_fraud,
    fraud_type

from source
