{% macro redshift__get_external_build_plan(source_node) %}
    {% set build_plan = [] %}
    {% set create_or_replace = var('ext_full_refresh', false) or not dbt_external_tables.redshift_is_ext_tbl() %}
    {% if create_or_replace %}
        {% set build_plan = [
            dbt_external_tables.dropif(source_node),
            dbt_external_tables.create_external_table(source_node)
        ] %}
    {% endif %}

    {% set statements = build_plan + dbt_external_tables.refresh_external_table(source_node) %}
    {% set standalone_statements = [] %}
    {# Redshift Spectrum DDL must run outside a transaction. #}
    {% for sql in statements %}
        {% do standalone_statements.append('begin; commit;\n' ~ sql) %}
    {% endfor %}
    {% do return(standalone_statements) %}
{% endmacro %}
