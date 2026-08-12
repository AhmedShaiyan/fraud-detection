{#
    Single source of truth for what makes a Bronze transaction row valid.

    Everything downstream is derived from ONE list of (reason, condition)
    pairs: slv_quarantine keeps the rows where at least one condition fires,
    slv_authorizations/slv_settlements keep the rows where none do. Writing
    those two filters independently is the failure mode this macro exists to
    prevent - the moment they disagree, a row either lands in both places or,
    far worse, in neither, and disappears with nothing to show it ever
    arrived.

    Two rules every condition here has to follow:

    1. Never evaluate to NULL. Each one is null-guarded (`amount is null or
       amount <= 0`, not just `amount <= 0`), because `not (NULL)` is NULL,
       which is not true - so a row failing an unguarded check would be
       filtered out of Silver AND out of quarantine at the same time. That is
       exactly the silent drop assert_no_bronze_rows_dropped.sql tests for.

    2. Stay event-type agnostic. These conditions run against every Bronze
       row, AUTH and SETTLEMENT alike, so they can only cover columns both
       event types populate. Settlement-specific rules (e.g. a settlement must
       carry an auth_transaction_id) belong in that model's own dbt tests, not
       here - AUTHs legitimately have a null auth_transaction_id.

    UNKNOWN_EVENT_TYPE has no counterpart in the producer's defect list. It is
    here because the Silver models split Bronze on event_type: a row whose
    event_type is neither AUTH nor SETTLEMENT matches neither model, and
    without this check it would not be quarantined either. It is the check
    that makes "no silent drops" true rather than nearly true.
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
    Array of the reasons this row fails, empty for a clean row. try_cast, not
    cast: Databricks SQL runs with ANSI mode on, where casting an unparseable
    string to timestamp raises rather than returning NULL - which would abort
    the whole model on the very rows it is supposed to be catching.
#}
{% macro transaction_quarantine_reasons() %}
array_compact(array(
    {%- for reason, condition in transaction_validity_checks() %}
    case when {{ condition }} then '{{ reason }}' end{{ "," if not loop.last }}
    {%- endfor %}
))
{% endmacro %}


{#
    Validity is *defined as* an empty reasons array, rather than being a
    second hand-written expression over the same conditions. Deriving one from
    the other is what guarantees the Silver filter and the quarantine filter
    are exact complements and cannot drift apart.
#}
{% macro transaction_is_valid() %}
(size({{ transaction_quarantine_reasons() }}) = 0)
{% endmacro %}
