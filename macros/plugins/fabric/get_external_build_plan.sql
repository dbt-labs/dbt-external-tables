{% macro fabric__get_external_build_plan(source_node, relation) %}

    {% set build_plan = [] %}

    {% set old_relation = load_relation(relation) %}

    {% set create_or_replace = (old_relation is none or var('ext_full_refresh', false)) %}

    {% if create_or_replace %}
        {% set build_plan = build_plan + [
            dbt_external_tables.dropif(source_node, relation),
            dbt_external_tables.create_external_table(source_node, relation)
        ] %}
    {% else %}
        {% set build_plan = build_plan + dbt_external_tables.refresh_external_table(source_node, relation) %}
    {% endif %}
    {% do return(build_plan) %}

{% endmacro %}
