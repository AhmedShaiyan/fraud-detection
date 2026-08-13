-- Current merchant master: latest attributes seen per merchant_id in the AUTH
-- stream (there's no separate merchant dimension feed). Reads
-- slv_authorizations, not Bronze, so a quarantined malformed row can't
-- rewrite a merchant's name/MCC.
--
-- transaction_id tie-break in the ORDER BY is load-bearing: feeds
-- merchants_snapshot's check strategy, and without a deterministic tie-break,
-- two same-event_time auths could flip which row is "current" every run.

with ranked as (

    select
        merchant_id,
        merchant_name,
        mcc,
        mcc_description,
        country,
        event_time,
        row_number() over (
            partition by merchant_id
            order by event_time desc, transaction_id desc
        ) as rn
    from {{ ref('slv_authorizations') }}
    where merchant_id is not null

)

select
    merchant_id,
    merchant_name,
    mcc,
    mcc_description,
    country,
    event_time as last_seen_at
from ranked
where rn = 1
