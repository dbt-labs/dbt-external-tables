{% macro create_external_table(source_node) %}
    {{ adapter.dispatch('create_external_table', 'dbt_external_tables')(source_node) }}
{% endmacro %}

{% macro default__create_external_table(source_node) %}
    {% do exceptions.raise_compiler_error('External table creation is not implemented for adapter ' ~ target.type ~ '.') %}
{% endmacro %}
