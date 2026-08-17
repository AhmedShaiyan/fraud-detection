{{
  config(
    unique_key='quarantine_key',
    merge_exclude_columns=['quarantined_at']
  )
}}

-- Bronze rows that fail macros/transaction_validity.sql, held rather than
-- dropped. slv_authorizations/slv_settlements take the exact complement, so
-- every Bronze row lands in exactly one of the three (see
-- tests/assert_no_bronze_rows_dropped.sql,
-- tests/assert_quarantine_disjoint_from_silver.sql).
--
-- Values kept RAW (no casts) so the evidence isn't destroyed - except
-- card_id, still hashed like Silver.
--
-- unique_key is quarantine_key, not transaction_id: null transaction_id is
-- itself a quarantine reason, and a merge on a null key never matches.
-- merge_exclude_columns keeps quarantined_at at first-failure time, not
-- last dbt run.

with source as (

    select *
    from {{ source('bronze', 'transactions_raw') }}

    {% if is_incremental() %}
    -- coalesce, unlike the sibling Silver models: an empty first build (no
    -- dirty rows yet) would otherwise leave max(_ingested_at) NULL forever.
    where _ingested_at > coalesce(
        (select max(_ingested_at) from {{ this }}),
        timestamp '1970-01-01'
    )
    {% endif %}

),

checked as (

    -- The reasons array is computed once and both the filter below and the
    -- stored column read from it, so the row set and the explanation of why
    -- each row is in it cannot disagree.
    select
        *,
        {{ transaction_quarantine_reasons() }} as quarantine_reasons
    from source

)

select

    sha2(concat_ws('||',
        coalesce(transaction_id, ''),
        coalesce(event_type, ''),
        coalesce(event_time, ''),
        coalesce(card_id, ''),
        coalesce(merchant_id, ''),
        coalesce(cast(amount as string), ''),
        coalesce(currency, ''),
        coalesce(_source_file, '')
    ), 256) as quarantine_key,

    quarantine_reasons,
    current_timestamp() as quarantined_at,

    transaction_id,
    event_type,
    event_time,
    auth_transaction_id,
    sha2(card_id, 256) as card_id_hash,
    merchant_id,
    merchant_name,
    mcc,
    mcc_description,
    amount,
    currency,
    country,
    lat,
    lon,
    channel,
    pos_entry_mode,
    card_present,
    auth_code,
    is_fraud,
    fraud_type,
    _ingested_at,
    _source_file

from checked
where size(quarantine_reasons) > 0
