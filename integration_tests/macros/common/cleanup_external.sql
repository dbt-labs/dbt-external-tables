{% macro cleanup_external() %}
    {{ return(adapter.dispatch('cleanup_external', dbt_external_tables._get_dbt_external_tables_namespaces())()) }}
{% endmacro %}

{% macro default__cleanup_external() %}
    {% do log('No cleanup necessary, skipping', info = true) %}
    {# noop #}
{% endmacro %}
