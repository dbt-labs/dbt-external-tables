{% macro dropif(source_node) %}
    {{ adapter.dispatch('dropif', 'dbt_external_tables')(source_node) }}
{% endmacro %}

{% macro default__dropif(source_node) %}
    {% do exceptions.raise_compiler_error('Dropping external tables is not implemented for adapter ' ~ target.type ~ '.') %}
{% endmacro %}
