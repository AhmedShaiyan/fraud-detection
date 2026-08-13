-- Half of the no-silent-drops guarantee: a transaction is either clean (in
-- Silver) or bad (in quarantine), never both - both sides filter on
-- macros/transaction_validity.sql, one negated. Fails if any transaction_id
-- appears in both. Null transaction_ids are excluded (covered by
-- construction: a null id is itself a quarantine reason).

with quarantined as (

    select transaction_id
    from {{ ref('slv_quarantine') }}
    where transaction_id is not null

),

silver as (

    select transaction_id from {{ ref('slv_authorizations') }}
    union all
    select transaction_id from {{ ref('slv_settlements') }}

)

select
    quarantined.transaction_id,
    count(*) as appearances_in_silver
from quarantined
join silver
    on quarantined.transaction_id = silver.transaction_id
group by quarantined.transaction_id
