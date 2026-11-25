{% macro snowflake__get_external_build_plan(source_node) %}
    {% set create_or_replace = var('ext_full_refresh', false) or not dbt_external_tables.snowflake_external_relation_exists() %}

    {% if source_node.external.get('snowpipe', none) is not none %}
        {% if create_or_replace %}
            {% do return([
                dbt_external_tables.snowflake_create_empty_table(source_node),
                dbt_external_tables.snowflake_get_copy_sql(source_node, explicit_transaction=true),
                dbt_external_tables.snowflake_create_snowpipe(source_node)
            ]) %}
        {% endif %}
        {% do return(dbt_external_tables.snowflake_refresh_snowpipe(source_node)) %}
    {% endif %}

    {% if create_or_replace %}
        {% do return([dbt_external_tables.create_external_table(source_node)]) %}
    {% endif %}
    {% do return(dbt_external_tables.refresh_external_table(source_node)) %}
{% endmacro %}
