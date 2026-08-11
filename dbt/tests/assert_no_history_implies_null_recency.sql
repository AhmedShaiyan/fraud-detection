-- A card's first-ever transaction has no prior amounts to average and no
-- prior timestamp to measure recency from. Fails if any has_history=false
-- row has a non-null amount_avg_24h or minutes_since_last_txn.

select *
from {{ ref('fct_card_velocity_features') }}
where has_history = false
  and (amount_avg_24h is not null or minutes_since_last_txn is not null)
