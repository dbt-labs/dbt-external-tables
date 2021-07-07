{% macro exit_transaction() %}
    {{ return(adapter.dispatch('exit_transaction', 'dbt_external_tables')()) }}
{% endmacro %}

{% macro default__exit_transaction() %}
    {{ return('') }}
{% endmacro %}
