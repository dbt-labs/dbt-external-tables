{% macro refresh_external_table(source_node) %}
    {{ return(adapter.dispatch('refresh_external_table', 
        packages = dbt_external_tables._get_dbt_external_tables_namespaces()) 
        (source_node)) }}
{% endmacro %}

{% macro default__refresh_external_table(source_node) %}
    {% do return([]) %}
{% endmacro %}
