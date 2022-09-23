{% macro spark__get_external_build_plan(source_node) %}

    {% set build_plan = [] %}

    {% set old_relation = adapter.get_relation(
        database = none,
        schema = source_node.schema,
        identifier = source_node.identifier
    ) %}

    {% set create_or_replace = (old_relation is none or var('ext_full_refresh', false)) %}

    {% if create_or_replace %}
        {% set build_plan = build_plan + [
            dbt_external_tables.create_external_schema(source_node),
            dbt_external_tables.dropif(source_node), 
            dbt_external_tables.create_external_table(source_node)
        ] %}
    {% else %}
        {% set build_plan = build_plan + dbt_external_tables.refresh_external_table(source_node) %}
    {% endif %}

    {% set recover_partitions = dbt_external_tables.recover_partitions(source_node) %}
    {% if recover_partitions %}
    {% set build_plan = build_plan + [
        recover_partitions
    ] %}
    {% endif %}

    {% do return(build_plan) %}

{% endmacro %}
