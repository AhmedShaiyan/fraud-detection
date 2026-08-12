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
    Type-2 SCD history for the merchant master. Merchants rebrand and get
    recategorized (producer.py's MERCHANT_DRIFT_RATE simulates ~2% of them
    doing so per run), and a fact table joined to today's merchant attributes
    would silently restate last month's transactions under this month's name.
    This snapshot versions instead of overwriting, so a transaction can be
    joined to the merchant as it was when the transaction happened.

    strategy='check' rather than 'timestamp': the source has no reliable
    updated_at for merchant *attributes*. last_seen_at moves every time the
    merchant transacts, which is not the same thing as the merchant changing -
    a timestamp strategy on it would cut a new version on ordinary activity.

    check_cols is exactly the pair that can actually drift. mcc_description is
    a lookup off mcc and always moves with it, so checking it would only add a
    second way to detect the same change. country and merchant_id are fixed at
    creation.

    Columns are listed explicitly rather than select *: it keeps last_seen_at
    out of the snapshot. Under the check strategy an unchanged row is not
    updated, so a stored last_seen_at would freeze at whatever it was when the
    version was cut and then quietly lie. dbt_valid_from/dbt_valid_to already
    carry the timeline this table is for.

    invalidate_hard_deletes is deliberately left off. A merchant absent from
    the current view has not been deleted - stg_merchants_current is built
    from the full auth history, so absence would only mean the merchant never
    transacted at all. Treating that as a deletion would close out live
    merchants on any partial rebuild of Silver.

    dbt 1.9 (dbt-databricks 1.9.4) still takes target_database/target_schema
    here; dbt 1.10 renames them to database/schema.
#}

select
    merchant_id,
    merchant_name,
    mcc,
    mcc_description,
    country
from {{ ref('stg_merchants_current') }}

{% endsnapshot %}
