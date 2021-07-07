{% macro refresh_external_table(source_node) %}
    {{ return(adapter.dispatch('refresh_external_table', 'dbt_external_tables')(source_node)) }}
{% endmacro %}

{% macro default__refresh_external_table(source_node) %}
    {% do return([]) %}
{% endmacro %}
