{% macro dropif(node) %}
    {{ adapter.dispatch('dropif', 
        packages = dbt_external_tables._get_dbt_external_tables_namespaces()) 
        (node) }}
{% endmacro %}

{% macro default__dropif() %}
    {{ exceptions.raise_compiler_error(
        "Dropping external tables is not implemented for the default adapter"
    ) }}
{% endmacro %}
