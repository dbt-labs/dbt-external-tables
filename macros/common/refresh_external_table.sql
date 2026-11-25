{% macro refresh_external_table(source_node, relation) %}
    {% do return(adapter.dispatch('refresh_external_table', 'dbt_external_tables')(source_node, relation)) %}
{% endmacro %}

{% macro default__refresh_external_table(source_node, relation) %}
    {% do return([]) %}
{% endmacro %}
