-- A declined auth is a normal outcome (the issuer said no), not a
-- reconciliation break. Fails if any AUTH_DECLINED row is flagged is_break.

select *
from {{ ref('fct_reconciliation') }}
where recon_status = 'AUTH_DECLINED'
  and is_break = true
