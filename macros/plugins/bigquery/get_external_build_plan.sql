{% macro bigquery__get_external_build_plan(source_node) %}
    {% set old_relation = load_relation(this) %}
    {% if old_relation is none or var('ext_full_refresh', false) %}
        {% set sql = [dbt_external_tables.create_external_table(source_node)] %}
    {% else %}
        {% set sql = dbt_external_tables.refresh_external_table(source_node) %}
    {% endif %}

    {# Column descriptions and policy tags also need updates when there is no DDL. #}
    {% do return({'sql': sql, 'post_macro': 'dbt_external_tables.bigquery__update_external_table_columns'}) %}
{% endmacro %}
