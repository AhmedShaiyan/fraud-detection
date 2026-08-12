-- The other half of the no-silent-drops guarantee: every Bronze row reaches
-- somewhere. Disjointness alone would still be satisfied by a row that landed
-- in neither place, which is the more dangerous failure - a row that vanishes
-- leaves nothing behind to notice.
--
-- This is the test that justifies the UNKNOWN_EVENT_TYPE check in
-- macros/transaction_validity.sql: the Silver models split Bronze on
-- event_type, so without that check a row with an unrecognized event_type
-- would match neither Silver model and fail no validity check either, and
-- would show up here.
--
-- Written as anti-joins rather than a count reconciliation (bronze total =
-- auths + settlements + quarantine) on purpose: the Silver models merge on
-- transaction_id, so a duplicate id landed twice in Bronze collapses to one
-- Silver row and would break a count comparison while nothing was actually
-- dropped. Existence is the property being asserted, so test existence.
--
-- Null transaction_ids are excluded because they cannot be matched by id.
-- They are covered by construction: NULL_TRANSACTION_ID is itself a
-- quarantine reason, so such a row is always captured.
--
-- Expect this to fail if it runs against a Bronze table that has been loaded
-- since the last dbt run - it is a post-transform gate, not a standalone one.

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
