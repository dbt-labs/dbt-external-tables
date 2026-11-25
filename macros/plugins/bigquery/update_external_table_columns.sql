{% macro bigquery__update_external_table_columns(node) %}
    {% do adapter.update_columns(this, node.columns) %}
{% endmacro %}
