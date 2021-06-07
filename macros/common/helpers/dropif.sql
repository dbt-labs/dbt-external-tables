{% macro dropif(node) %}
    {{ adapter.dispatch('dropif', 'dbt_external_tables')(node) }}
{% endmacro %}

{% macro default__dropif() %}
    {{ exceptions.raise_compiler_error(
        "Dropping external tables is not implemented for the default adapter"
    ) }}
{% endmacro %}
