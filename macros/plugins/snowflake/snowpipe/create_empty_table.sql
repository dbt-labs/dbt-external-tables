{% macro snowflake_create_empty_table(source_node) %}

    {%- set columns = source_node.columns.values() %}

    create or replace table {{source(source_node.source_name, source_node.name)}} (
        {% if columns|length == 0 %}
            value variant,
        {% else -%}
        {%- for column in columns -%}
            {%- if column.quote -%}
                {{ adapter.quote(column.name) }} {{column.data_type}},
            {% else %}
                {{column.name}} {{column.data_type}},
            {% endif %}
        {% endfor -%}
        {% endif %}
            metadata_filename varchar,
            metadata_file_row_number bigint,
            metadata_file_last_modified timestamp,
            _dbt_copied_at timestamp
    );

{% endmacro %}