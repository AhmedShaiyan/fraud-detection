-- txn_count_24h is a superset window of txn_count_1h, so it can never be
-- smaller. Fails if any row's 24h count is less than its 1h count.

select *
from {{ ref('fct_card_velocity_features') }}
where txn_count_24h < txn_count_1h
