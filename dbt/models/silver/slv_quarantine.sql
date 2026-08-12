{{
  config(
    unique_key='quarantine_key',
    merge_exclude_columns=['quarantined_at']
  )
}}

-- Bronze rows that fail any check in macros/transaction_validity.sql, held
-- rather than dropped. slv_authorizations/slv_settlements take the exact
-- complement of this set (same macro, negated), so every Bronze row lands in
-- exactly one of the three - see tests/assert_no_bronze_rows_dropped.sql and
-- tests/assert_quarantine_disjoint_from_silver.sql, which pin both halves of
-- that guarantee.
--
-- Values are kept RAW: no cast(event_time as timestamp), no cast(amount as
-- decimal). Casting is what Silver does to rows it trusts; doing it here
-- would turn the unparseable timestamp this table exists to capture into a
-- NULL and destroy the evidence needed to diagnose or replay the row. The one
-- exception is card_id, hashed exactly as in Silver - the raw PAN-like
-- identifier never leaves Bronze, and a row being malformed is not a reason
-- to relax that.
--
-- unique_key is quarantine_key, NOT the folder default transaction_id:
-- `transaction_id is null` is itself a quarantine reason, and a merge on a
-- null key never matches, so those rows would be re-inserted on every run.
-- The surrogate hashes the row's identifying columns plus _source_file, which
-- makes re-processing the same landed file idempotent (CLAUDE.md: dedupe on
-- the natural key where COPY INTO file tracking doesn't cover it). Two
-- byte-identical bad rows in one file collapse to one, which is the intended
-- reading of "the same bad record".
--
-- merge_exclude_columns keeps quarantined_at at its original value when a row
-- is re-seen, so the column means "when this first failed", not "when dbt
-- last touched it".

with source as (

    select *
    from {{ source('bronze', 'transactions_raw') }}

    {% if is_incremental() %}
    -- coalesce, unlike the sibling Silver models: quarantine is legitimately
    -- empty whenever a batch is clean (and --dirty-rate defaults to 0), and
    -- `_ingested_at > NULL` is NULL for every row. Without the floor, an
    -- empty initial build would leave this table unable to ever accept its
    -- first row.
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
