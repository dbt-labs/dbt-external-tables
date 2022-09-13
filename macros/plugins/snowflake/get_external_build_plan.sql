{% macro snowflake__get_external_build_plan(source_node) %}
    {% do log('Warn: you are running the patched version of get_external_build_plan', info=true ) %}

    {% set build_plan = [] %}
    
    {% set old_relation = adapter.get_relation(
        database = source_node.database,
        schema = source_node.schema,
        identifier = source_node.identifier
    ) %}
    
    {% set create_or_replace = old_relation is none %}
    {% set ext_full_refresh = var('ext_full_refresh', false) %}


    {% if source_node.external.get('snowpipe', none) is not none %}
    
        {% set build_plan = build_plan + [
             dbt_external_tables.snowflake_create_empty_table(source_node),
             dbt_external_tables.snowflake_create_snowpipe(source_node)
            ] %}

        {% if ext_full_refresh %}
            {% set build_plan = build_plan + [dbt_external_tables.snowflake_get_copy_sql(source_node, explicit_transaction=true)] %}
        {% endif %}
    {% else %}
    
        {% if create_or_replace %}
            {% set build_plan = build_plan + [dbt_external_tables.create_external_table(source_node)] %}
        {% else %}
            {% set build_plan = build_plan + dbt_external_tables.refresh_external_table(source_node) %}
        {% endif %}
        
    {% endif %}

    {% do return(build_plan) %}

{% endmacro %}
