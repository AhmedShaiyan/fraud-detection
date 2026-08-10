-- MATCHED is only valid when the drift cleared both the percentage
-- tolerance and the absolute ceiling. Fails if any MATCHED row's drift
-- exceeds the ceiling, which would mean the status logic let it through.

select *
from {{ ref('fct_reconciliation') }}
where recon_status = 'MATCHED'
  and abs(amount_diff) > {{ var('recon_amount_ceiling_abs', 100.00) }}
