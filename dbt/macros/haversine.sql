{#
    Great-circle distance in km between two lat/lon points (decimal
    degrees), used to derive implied_speed_kmh in fct_card_velocity_features.
#}
{% macro haversine_km(lat1, lon1, lat2, lon2) %}
(
    2 * 6371 * asin(
        sqrt(
            pow(sin(radians({{ lat2 }} - {{ lat1 }}) / 2), 2)
            + cos(radians({{ lat1 }})) * cos(radians({{ lat2 }}))
              * pow(sin(radians({{ lon2 }} - {{ lon1 }}) / 2), 2)
        )
    )
)
{% endmacro %}
