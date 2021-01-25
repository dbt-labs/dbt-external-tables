{% macro _get_dbt_external_tables_namespaces() %}
  {% set override_namespaces = var('dbt_external_tables_dispatch_list', []) %}
  {% do return(override_namespaces + ['dbt_external_tables']) %}
{% endmacro %}
