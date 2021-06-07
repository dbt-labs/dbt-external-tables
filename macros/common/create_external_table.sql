{% macro create_external_table(source_node) %}
    {{ adapter.dispatch('create_external_table', 'dbt_external_tables')(source_node) }}
{% endmacro %}

{% macro default__create_external_table(source_node) %}
    {{ exceptions.raise_compiler_error("External table creation is not implemented for the default adapter") }}
{% endmacro %}
