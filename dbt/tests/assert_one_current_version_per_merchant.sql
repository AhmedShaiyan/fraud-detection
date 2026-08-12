-- The core Type-2 SCD invariant: at most one open version per key. A merchant
-- with two rows where dbt_valid_to is null means a previous version was never
-- closed out, and every downstream join to "the current merchant" would
-- silently fan out, duplicating transactions.
--
-- Fails if any merchant_id has more than one open version.

select
    merchant_id,
    count(*) as open_versions
from {{ ref('merchants_snapshot') }}
where dbt_valid_to is null
group by merchant_id
having count(*) > 1
