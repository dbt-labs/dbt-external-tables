{#-- Rockset does not support `create table` sql, so creating tables from external sources is handled --#}
{#-- 100% in the adapter python code, the resulting sql / build plan is a noop. --#}
{% macro rockset__get_external_build_plan(source_node) %}
    
    {%- set external = source_node.external -%}
    {%- set options = external.options -%}
    {% set unused = adapter.create_table_from_external(
        schema = source_node.schema,
        identifier = source_node.identifier,
        options = options
    ) %}
    {% do return([]) %}

{% endmacro %}
