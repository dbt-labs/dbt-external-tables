{% macro snowflake__get_external_build_plan(source_node, relation=none) %}

    {% set build_plan = [] %}
    
    {% set old_relation = adapter.get_relation(
        database = relation.database if relation is not none else source_node.database,
        schema = relation.schema if relation is not none else source_node.schema,
        identifier = relation.identifier if relation is not none else source_node.identifier
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
    
        {% if relation is not none %}
            {# Per-node staging uses precreated schemas and the exact selected relation. #}
            {% if create_or_replace %}
                {% do return([dbt_external_tables.snowflake__create_external_table(source_node, relation=relation)]) %}
            {% endif %}
            {% do return(dbt_external_tables.snowflake__refresh_external_table(source_node, relation=relation)) %}
        {% endif %}

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
