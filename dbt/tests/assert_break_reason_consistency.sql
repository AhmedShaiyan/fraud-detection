-- break_reason should be populated exactly when is_break is true, and null
-- exactly when it's false. Fails on either mismatch direction.

select *
from {{ ref('fct_reconciliation') }}
where (is_break = true and break_reason is null)
   or (is_break = false and break_reason is not null)
