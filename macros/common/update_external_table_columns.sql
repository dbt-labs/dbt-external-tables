{% macro update_external_table_columns(source_node) %}
    {{ return(adapter.dispatch('update_external_table_columns', 'dbt_external_tables')(source_node)) }}
{% endmacro %}

{% macro default__update_external_table_columns(source_node) %}

{% endmacro %}
