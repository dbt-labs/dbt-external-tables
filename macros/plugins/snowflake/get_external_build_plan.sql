{% macro snowflake__get_external_build_plan(source_node) %}

    {% set build_plan = [] %}
    
    {% set old_relation = adapter.get_relation(
        database = source_node.database,
        schema = source_node.schema,
        identifier = source_node.identifier
    ) %}
    
    {% set create_or_replace = (old_relation is none or var('ext_full_refresh', false)) %}

    {% if source_node.external.get('snowpipe', none) is not none %}
    
        {% if create_or_replace %}
            {% set build_plan = build_plan + [
                dbt_external_tables.create_external_schema(source_node),
                dbt_external_tables.snowflake_create_empty_table(source_node),
                dbt_external_tables.snowflake_get_copy_sql(source_node, explicit_transaction=true),
                dbt_external_tables.snowflake_create_snowpipe(source_node)
            ] %}
        {% else %}
            {% set build_plan = build_plan + dbt_external_tables.snowflake_refresh_snowpipe(source_node) %}
        {% endif %}
            
    {% else %}
    
        {% if create_or_replace %}
            {% set build_plan = build_plan + [
                dbt_external_tables.create_external_schema(source_node),
                dbt_external_tables.create_external_table(source_node)
            ] %}
        {% else %}
            {% set build_plan = build_plan + dbt_external_tables.refresh_external_table(source_node) %}
        {% endif %}
        
    {% endif %}

    {% do return(build_plan) %}

{% endmacro %}
