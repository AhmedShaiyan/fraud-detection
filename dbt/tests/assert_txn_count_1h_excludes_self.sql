-- txn_count_1h must exclude the current row, so it can never reach the
-- card's total row count. Fails if any row's txn_count_1h is >= that total.

select v.*
from {{ ref('fct_card_velocity_features') }} v
join (
    select card_id_hash, count(*) as card_total_txn_count
    from {{ ref('fct_card_velocity_features') }}
    group by card_id_hash
) totals
    on v.card_id_hash = totals.card_id_hash
where v.txn_count_1h >= totals.card_total_txn_count
