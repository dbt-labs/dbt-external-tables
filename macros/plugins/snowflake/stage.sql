{% macro snowflake__stage(node) %}
    {% if node.external.get('snowpipe', none) is not none %}
        {% do exceptions.raise_compiler_error('Per-node staging does not support Snowpipes. Use stage_external_sources for this source.') %}
    {% endif %}
    {% if this is undefined or this is none %}
        {% do exceptions.raise_compiler_error('Call dbt_external_tables.stage with dbt run-operation --per-node.') %}
    {% endif %}

    {% do return(dbt_external_tables.snowflake__get_external_build_plan(node, relation=this)) %}
{% endmacro %}
