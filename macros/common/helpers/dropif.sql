{% macro dropif(source_node, relation) %}
    {{ adapter.dispatch('dropif', 'dbt_external_tables')(source_node, relation) }}
{% endmacro %}

{% macro default__dropif(source_node, relation) %}
    {% do exceptions.raise_compiler_error('Dropping external tables is not implemented for adapter ' ~ target.type ~ '.') %}
{% endmacro %}
