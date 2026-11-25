{% macro stage(node) %}
    {% if node.resource_type != 'source' or not node.external %}
        {% do return([]) %}
    {% endif %}

    {% do return(adapter.dispatch('stage', 'dbt_external_tables')(node)) %}
{% endmacro %}

{% macro default__stage(node) %}
    {% do exceptions.raise_compiler_error('Per-node external source staging is supported on Snowflake. Use stage_external_sources for this adapter.') %}
{% endmacro %}
