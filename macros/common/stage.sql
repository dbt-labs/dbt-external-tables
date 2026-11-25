{% macro stage(node) %}
    {% if node.resource_type != 'source' or not node.external %}
        {% do return([]) %}
    {% endif %}
    {% if this is undefined or this is none %}
        {% do exceptions.raise_compiler_error('Call dbt_external_tables.stage with dbt run-operation --per-node.') %}
    {% endif %}

    {% do return(adapter.dispatch('stage', 'dbt_external_tables')(node)) %}
{% endmacro %}

{% macro default__stage(node) %}
    {% do return(dbt_external_tables.get_external_build_plan(node)) %}
{% endmacro %}
