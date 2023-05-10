{% macro snowflake_create_empty_table(source_node) %}

    {%- set columns = source_node.columns.values() %}

    create or replace transient table {{source(source_node.source_name, source_node.name)}} (
        {% if columns|length == 0 %}
        value variant,
        {% else -%}
        {%- for column in columns %}
        {{column.name}} {{column.data_type}},
        {% endfor -%}
        {% endif %}
        rsrc string not null,
        file_row_seq number not null,
        rldts timestamp not null
    );

{% endmacro %}
