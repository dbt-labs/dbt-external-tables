{% macro snowflake__create_external_table(source_node) %}

    {%- set columns = source_node.columns.values() -%}
    {%- set external = source_node.external -%}
    {%- set partitions = external.partitions -%}
    {%- set infer_schema = external.infer_schema -%}
    {%- set ignore_case = external.ignore_case or false  -%}

    {%- set ff_opt_dict = dbt_external_tables.get_ff(external.file_format) -%}
    {%- set is_csv_ff = ff_opt_dict['type']|default('csv')|lower == 'csv' -%}

    {%- if infer_schema -%}
        {# Initialize inference file format #}
        {%- set inference_ff_name = ff_name -%}
        {% if not ff_name or is_csv_ff %}
            {# INFER_SCHEMA requires a named file format, and different header options for CSVs. #}
            {# Create a temporary file format with the correct options. #}
            {%- set inference_ff_name = '_temp_ff_' ~ source_node.source_name ~ '_' ~ source_node.name -%}
            {% set temp_ff_opt_dict = ff_opt_dict.copy() %}
            {% if is_csv_ff %}
                {% do temp_ff_opt_dict.pop('skip_header', none) %}
                {% do temp_ff_opt_dict.update({'parse_header': true}) %}
            {% endif %}
            {%- set file_format_query %}
                create or replace temporary file format {{inference_ff_name}} 
                    {{ temp_ff_opt_dict.items() | map('join', '=') | join(' ') }}
            {%- endset -%}
            {% do run_query(file_format_query) %}
        {%- endif -%}

        {% set query_infer_schema %}
            select * from table( infer_schema( location=>'{{external.location}}', file_format=>'{{inference_ff_name}}', ignore_case=> {{ ignore_case }}) )
        {% endset %}

        {% set columns_infer = run_query(query_infer_schema) %}
        {% set columns = [] %}
        {% for row in columns_infer %}
            {% do columns.append({
                'name': row.COLUMN_NAME,
                'data_type': row.TYPE,
                'quote': True
            })
            %}
        {% endfor %}
    {%- endif %}


{# https://docs.snowflake.net/manuals/sql-reference/sql/create-external-table.html #}
{# This assumes you have already created an external stage #}
{% set ddl %}
    create or replace external table {{source(source_node.source_name, source_node.name)}}
    {%- if columns or partitions -%}
    (
        {%- if partitions -%}{%- for partition in partitions %}
            {{partition.name}} {{partition.data_type}} as {{partition.expression}}{{- ',' if not loop.last or columns|length > 0 -}}
        {%- endfor -%}{%- endif -%}

        {%- for column in columns %}
            {%- set column_quoted = adapter.quote(column.name) if column.quote else column.name %}
            {%- set column_alias -%}
                {%- if 'alias' in column and column.quote -%}
                    {{adapter.quote(column.alias)}}
                {%- elif 'alias' in column -%}
                    {{column.alias}}
                {%- elif column_quoted == '"VALUE"' -%}
                    {# Avoid using reserved word 'VALUE' as alias #}
                    "_VALUE"
                {%- else -%}
                    {{column_quoted}}
                {%- endif -%}
            {%- endset %}
            {%- set col_expression -%}
                {%- if column.expression -%}
                    {{column.expression}}
                {%- else -%}
                    {%- if ignore_case -%}
                    {%- set col_id = 'value:c' ~ loop.index if is_csv_ff else 'GET_IGNORE_CASE($1, ' ~ "'"~ column.name ~"'"~ ')' -%}
                    {%- else -%}
                    {%- set col_id = 'value:c' ~ loop.index if is_csv_ff else 'value:' ~ column_quoted -%}
                    {%- endif -%}
                    (case when is_null_value({{col_id}}) or lower({{col_id}}) = 'null' then null else {{col_id}} end)
                {%- endif -%}
            {%- endset %}
            {{column_alias}} {{column.data_type}} as ({{col_expression}}::{{column.data_type}})
            {{- ',' if not loop.last -}}
        {% endfor %}

    )
    {%- endif -%}
    {% if partitions %} partition by ({{partitions|map(attribute='name')|join(', ')}}) {% endif %}
    location = {{external.location}} {# stage #}
    {% if external.auto_refresh in (true, false) -%}
      auto_refresh = {{external.auto_refresh}}
    {%- endif %}
    {% if external.aws_sns_topic -%}
      aws_sns_topic = '{{external.aws_sns_topic}}'
    {%- endif %}
    {% if external.table_format | lower == "delta" %}
      refresh_on_create = false
    {% endif %}
    {% if external.pattern -%} pattern = '{{external.pattern}}' {%- endif %}
    {% if external.integration -%} integration = '{{external.integration}}' {%- endif %}
    file_format = {{external.file_format}}
    {% if external.table_format -%} table_format = '{{external.table_format}}' {%- endif %}
{% endset %}
{#  #}
{# {{ log('ddl: ' ~ ddl, info=True) }} #}

{{ ddl }};

{% endmacro %}
