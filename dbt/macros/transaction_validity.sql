{#
    Single source of truth for what makes a Bronze transaction row valid.
    slv_quarantine keeps rows where any (reason, condition) pair fires;
    slv_authorizations/slv_settlements keep rows where none do - deriving
    both filters from one list is what keeps them exact complements.

    Rules: (1) every condition must be null-guarded (`amount is null or
    amount <= 0`, not just `amount <= 0`) since `not (NULL)` is NULL, which
    would silently drop a row from both Silver and quarantine. (2) conditions
    must stay event-type agnostic (columns both AUTH and SETTLEMENT
    populate); type-specific rules belong in that model's own tests.

    UNKNOWN_EVENT_TYPE has no producer-side defect counterpart - it exists
    because the Silver split is on event_type, and a row matching neither
    AUTH nor SETTLEMENT would otherwise match neither model and vanish.
#}

{% macro transaction_validity_checks() %}
    {{ return([
        ('NULL_TRANSACTION_ID',    'transaction_id is null'),
        ('NULL_CARD_ID',           'card_id is null'),
        ('NONPOSITIVE_AMOUNT',     'amount is null or amount <= 0'),
        ('UNKNOWN_CURRENCY',       "currency is null or currency not in ('" ~ var('valid_currencies') | join("', '") ~ "')"),
        ('UNPARSEABLE_EVENT_TIME', 'event_time is null or try_cast(event_time as timestamp) is null'),
        ('UNKNOWN_EVENT_TYPE',     "event_type is null or event_type not in ('AUTH', 'SETTLEMENT')"),
    ]) }}
{% endmacro %}


{#
    Reasons array, empty for a clean row. try_cast, not cast: ANSI mode
    raises on an unparseable timestamp cast instead of returning NULL, which
    would abort the model on the rows it's supposed to catch.
#}
{% macro transaction_quarantine_reasons() %}
array_compact(array(
    {%- for reason, condition in transaction_validity_checks() %}
    case when {{ condition }} then '{{ reason }}' end{{ "," if not loop.last }}
    {%- endfor %}
))
{% endmacro %}


{# Validity = empty reasons array, so it can't drift from the quarantine filter. #}
{% macro transaction_is_valid() %}
(size({{ transaction_quarantine_reasons() }}) = 0)
{% endmacro %}
