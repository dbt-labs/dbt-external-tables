{% macro create_schema(source_node) %}
    {{ adapter.dispatch('create_schema', 'dbt_external_tables')(source_node) }}
{% endmacro %}

{% macro default__create_schema() %}
    {{ exceptions.raise_compiler_error(
        "Dropping external tables is not implemented for the default adapter"
    ) }}
{% endmacro %}