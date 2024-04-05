{% macro get_external_build_plan(source_node) %}
    {{ log('NOTHING TO SEE HERE', info=True) }}
    {{ log(">>>>>>>>>>>" ~ env_var("SNOWFLAKE_TEST_WHNAME"), info=True) }}
    {{ log(">>>>>>>>>>>" ~ env_var("REDSHIFT_TEST_PORT"), info=True) }}
    {{ return(adapter.dispatch('get_external_build_plan', 'dbt_external_tables')(source_node)) }}
{% endmacro %}

{% macro default__get_external_build_plan(source_node) %}
    {{ exceptions.raise_compiler_error("Staging external sources is not implemented for the default adapter") }}
{% endmacro %}
