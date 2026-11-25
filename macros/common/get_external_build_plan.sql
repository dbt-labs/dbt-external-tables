{% macro get_external_build_plan(source_node) %}
    {% do return(adapter.dispatch('get_external_build_plan', 'dbt_external_tables')(source_node)) %}
{% endmacro %}

{% macro default__get_external_build_plan(source_node) %}
    {% do exceptions.raise_compiler_error('External source staging is not implemented for adapter ' ~ target.type ~ '.') %}
{% endmacro %}
