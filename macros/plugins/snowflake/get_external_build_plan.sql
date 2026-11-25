{% macro snowflake__get_external_build_plan(source_node, relation) %}
    {% set old_relation = adapter.get_relation(
        database=relation.database,
        schema=relation.schema,
        identifier=relation.identifier
    ) %}
    {% set create_or_replace = old_relation is none or var('ext_full_refresh', false) %}

    {% if source_node.external.get('snowpipe', none) is not none %}
        {% if create_or_replace %}
            {% do return([
                dbt_external_tables.snowflake_create_empty_table(source_node, relation),
                dbt_external_tables.snowflake_get_copy_sql(source_node, relation, explicit_transaction=true),
                dbt_external_tables.snowflake_create_snowpipe(source_node, relation)
            ]) %}
        {% endif %}
        {% do return(dbt_external_tables.snowflake_refresh_snowpipe(source_node, relation)) %}
    {% endif %}

    {% if create_or_replace %}
        {% do return([dbt_external_tables.create_external_table(source_node, relation)]) %}
    {% endif %}
    {% do return(dbt_external_tables.refresh_external_table(source_node, relation)) %}
{% endmacro %}
