{% macro bigquery__update_external_table_columns(source_node) %}
    {%- set columns = source_node.columns -%}
    {%- set relation = source(source_node.source_name, source_node.name) -%}
    {%- do adapter.update_columns(relation, columns) -%}
{% endmacro %}
