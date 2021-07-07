{% macro prep_external() %}
    {{ return(adapter.dispatch('prep_external', 'dbt_external_tables')()) }}
{% endmacro %}

{% macro default__prep_external() %}
    {% do log('No prep necessary, skipping', info = true) %}
    {# noop #}
{% endmacro %}
