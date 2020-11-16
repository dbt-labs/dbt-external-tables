{% macro cleanup_external() %}
    {{ return(adapter.dispatch('cleanup_external', dbt_external_tables._get_dbt_external_tables_namespaces())()) }}
{% endmacro %}

{% macro default__cleanup_external() %}
    {% do log('No cleanup necessary, skipping', info = true) %}
    {# noop #}
{% endmacro %}

{% macro snowflake__cleanup_external() %}

    {% set unset_autocommit %}
        alter user {{ target.user }} unset autocommit;
    {% endset %}

    {% do log('Unsetting autocommit parameter for user ' ~ target.user, info = true) %}
    {% do run_query(unset_autocommit) %}
    
{% endmacro %}
