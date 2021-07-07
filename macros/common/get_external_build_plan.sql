{% macro get_external_build_plan(source_node) %}
    {{ return(adapter.dispatch('get_external_build_plan', 'dbt_external_tables')(source_node)) }}
{% endmacro %}

{% macro default__get_external_build_plan(source_node) %}
    {{ exceptions.raise_compiler_error("Staging external sources is not implemented for the default adapter") }}
{% endmacro %}
