-- Other half of the no-silent-drops guarantee: every Bronze row reaches
-- somewhere (disjointness alone doesn't rule out a row landing in neither
-- place). Justifies the UNKNOWN_EVENT_TYPE check in
-- macros/transaction_validity.sql - without it, an unrecognized event_type
-- would match no Silver model and no validity check.
--
-- Anti-joins, not a count reconciliation: a duplicate id landing twice in
-- Bronze merges to one Silver row and would break a count comparison though
-- nothing was dropped. Null transaction_ids excluded (covered by
-- construction). Post-transform gate - expect failures against Bronze
-- loaded since the last dbt run.

select
    bronze.transaction_id,
    bronze.event_type,
    bronze._source_file,
    bronze._ingested_at
from {{ source('bronze', 'transactions_raw') }} as bronze
where bronze.transaction_id is not null
  and not exists (
      select 1 from {{ ref('slv_authorizations') }} as a
      where a.transaction_id = bronze.transaction_id
  )
  and not exists (
      select 1 from {{ ref('slv_settlements') }} as s
      where s.transaction_id = bronze.transaction_id
  )
  and not exists (
      select 1 from {{ ref('slv_quarantine') }} as q
      where q.transaction_id = bronze.transaction_id
  )
