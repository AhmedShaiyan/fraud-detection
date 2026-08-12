-- Current merchant master: the latest attributes seen for each merchant_id
-- in the AUTH stream. There is no merchant dimension feed in this pipeline -
-- merchants are only ever observed through the transactions they appear on -
-- so "the merchant master" is a derived view, and this is where that
-- derivation is stated once.
--
-- Reads slv_authorizations rather than Bronze on purpose: a row that failed
-- validation is in slv_quarantine, and letting a malformed row set a
-- merchant's current name or MCC would let one bad record rewrite the
-- dimension. Quarantine gates the dimension, not just the fact tables.
--
-- The tie-break in the ORDER BY is load-bearing, not decoration. This view
-- feeds merchants_snapshot, which uses the check strategy: if two auths for
-- one merchant share the maximum event_time, an ORDER BY on event_time alone
-- lets Spark pick either row arbitrarily, and the snapshot would record a new
-- version every run as the "current" attributes flip-flopped between two rows
-- that never actually changed. transaction_id makes the pick deterministic.

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
