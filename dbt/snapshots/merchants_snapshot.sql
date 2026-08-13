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
    Type-2 SCD history for the merchant master (producer.py's
    MERCHANT_DRIFT_RATE simulates ~2% rebranding/recategorizing per run), so
    a transaction joins to the merchant as it was at transaction time.

    strategy='check', not 'timestamp': last_seen_at moves on every merchant
    transaction, not just attribute changes, so a timestamp strategy would
    cut a spurious new version on ordinary activity. check_cols is just
    merchant_name/mcc - mcc_description is a lookup off mcc, country/
    merchant_id are fixed at creation.

    Columns listed explicitly (not select *) to keep last_seen_at out - under
    'check', an unchanged row isn't updated, so a stored last_seen_at would
    freeze and lie. invalidate_hard_deletes is off: an absent merchant just
    hasn't transacted, not been deleted.

    dbt 1.9 (dbt-databricks 1.9.4) still takes target_database/target_schema;
    dbt 1.10 renames them to database/schema.
#}

select
    merchant_id,
    merchant_name,
    mcc,
    mcc_description,
    country
from {{ ref('stg_merchants_current') }}

{% endsnapshot %}
