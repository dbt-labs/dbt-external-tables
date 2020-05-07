{% macro exit_transaction() %}
    {{ return(adapter_macro('dbt_external_tables.exit_transaction')) }}
{% endmacro %}

{% macro default__exit_transaction() %}
    {# noop #}
{% endmacro %}

{% macro redshift__exit_transaction() %}
    {% do run_query('begin; commit;') %}
{% endmacro %}
