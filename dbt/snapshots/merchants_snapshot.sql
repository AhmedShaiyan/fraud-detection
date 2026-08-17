{% snapshot merchants_snapshot %}

{{
  config(
    target_database='fraud',
    target_schema='silver',
    unique_key='merchant_id',
    strategy='check',
    check_cols=['merchant_name', 'mcc']
  )
}}

{#
    Type-2 SCD for the merchant master (producer.py's MERCHANT_DRIFT_RATE
    simulates ~2% rebranding/recategorizing per run), so a transaction joins
    to the merchant as it was at transaction time.

    strategy='check' not 'timestamp': last_seen_at moves on every merchant
    transaction, not just attribute changes, which would cut spurious
    versions. check_cols is merchant_name/mcc only.

    Columns listed explicitly (not select *) to keep last_seen_at out - a
    stored value would freeze and lie under 'check' semantics.

    target_database/target_schema are dbt 1.9 naming; dbt 1.10 renames to
    database/schema.
#}

select
    merchant_id,
    merchant_name,
    mcc,
    mcc_description,
    country
from {{ ref('stg_merchants_current') }}

{% endsnapshot %}
