-- Non-fraud implied_speed_kmh must respect a physically plausible ceiling:
-- card-present pairs are ground-speed-limited at generation time (see
-- producer.py's GROUND_SPEED_KMH merchant re-pick), and CNP rows never
-- contribute to the calculation at all (see model header comment). Fails
-- if any non-fraud row's implied speed exceeds 900 km/h - well above
-- commercial ground speed, comfortably below the flight-speed ceiling
-- legitimate cross-country travel is gated at.

select *
from {{ ref('fct_card_velocity_features') }}
where fraud_type is null
  and implied_speed_kmh > 900
