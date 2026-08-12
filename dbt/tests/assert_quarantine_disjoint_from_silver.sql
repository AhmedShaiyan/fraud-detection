-- Half of the no-silent-drops guarantee: a transaction is either clean and in
-- Silver, or bad and in quarantine, never both. Both sides filter on the same
-- macro (macros/transaction_validity.sql), one negated, so an overlap means
-- those two filters have stopped being exact complements - the failure this
-- whole design is built to make impossible.
--
-- Fails if any transaction_id appears in slv_quarantine and in a Silver
-- model. Rows with a null transaction_id are excluded because they cannot be
-- compared by id at all; those are covered by construction (a null id is
-- itself a quarantine reason, so they can never reach a Silver model).

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
