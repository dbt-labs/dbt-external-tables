{% macro exit_transaction() %}
    {{ return(adapter.dispatch('exit_transaction', dbt_external_tables._get_dbt_external_tables_namespaces())()) }}
{% endmacro %}

{% macro default__exit_transaction() %}
    {{ return('') }}
{% endmacro %}

{% macro redshift__exit_transaction() %}
    {{ return('begin; commit;') }}
{% endmacro %}
