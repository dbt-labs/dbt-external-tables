{% macro snowflake__create_external_table(source_node) %}

    {%- set columns = source_node.columns.values() -%}
    {%- set external = source_node.external -%}
    {%- set partitions = external.partitions -%}

    {%- set is_csv = dbt_external_tables.is_csv(external.file_format) -%}

{# https://docs.snowflake.net/manuals/sql-reference/sql/create-external-table.html #}
{# This assumes you have already created an external stage #}
    create or replace external table {{source(source_node.source_name, source_node.name)}}
    {%- if columns or partitions -%}
    (
        {%- if partitions -%}{%- for partition in partitions %}
            {{partition.name}} {{partition.data_type}} as {{partition.expression}}{{- ',' if not loop.last or columns|length > 0 -}}
        {%- endfor -%}{%- endif -%}
        {%- for column in columns %}
            {%- set column_quoted = adapter.quote(column.name) if column.quote else column.name %}
            {%- set col_expression -%}
                {%- set col_id = 'value:c' ~ loop.index if is_csv else 'value:' ~ column_quoted -%}
                (case when is_null_value({{col_id}}) or lower({{col_id}}) = 'null' then null else {{col_id}} end)
            {%- endset %}
            {{column_quoted}} {{column.data_type}} as ({{col_expression}}::{{column.data_type}})
            {{- ',' if not loop.last -}}
        {% endfor %}
    )
    {%- endif -%}
    {% if partitions %} partition by ({{partitions|map(attribute='name')|join(', ')}}) {% endif %}
    location = {{external.location}} {# stage #}
    {% if external.auto_refresh in (true, false) -%}
      auto_refresh = {{external.auto_refresh}}
    {%- endif %}
    {% if external.pattern -%} pattern = '{{external.pattern}}' {%- endif %}
    {% if external.integration -%} integration = '{{external.integration}}' {%- endif %}
    file_format = {{external.file_format}}
    {% if external.table_format -%} table_format = '{{external.table_format}}' {%- endif %}
{% endmacro %}
