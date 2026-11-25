{% macro create_external_table(source_node, relation) %}
    {{ adapter.dispatch('create_external_table', 'dbt_external_tables')(source_node, relation) }}
{% endmacro %}

{% macro default__create_external_table(source_node, relation) %}
    {% do exceptions.raise_compiler_error('External table creation is not implemented for adapter ' ~ target.type ~ '.') %}
{% endmacro %}
