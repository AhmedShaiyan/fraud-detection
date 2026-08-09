{#
    Default dbt behavior concatenates target schema + custom schema
    (e.g. "silver_silver" or "dev_silver"). Override so a model's custom
    schema config is used verbatim, landing slv_* models in fraud.silver.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%}

        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
