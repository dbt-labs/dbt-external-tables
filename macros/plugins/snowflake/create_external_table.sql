{% macro snowflake__create_external_table(source_node) %}

    {%- set columns = source_node.columns.values() -%}
    {%- set external = source_node.external -%}
    {%- set partitions = external.partitions -%}
    {%- set infer_schema = external.infer_schema -%}
    {%- set ignore_case = external.ignore_case or false  -%}

    {% if infer_schema %}
        {% set query_infer_schema %}
            select * from table( infer_schema( location=>'{{external.location}}', file_format=>'{{external.file_format}}', ignore_case=> {{ ignore_case }}) )
        {% endset %}
        {% if execute %}
            {% set columns_infer = run_query(query_infer_schema) %}
        {% endif %}
    {% endif %}

    {%- set is_csv = dbt_external_tables.is_csv(external.file_format) -%}

{# https://docs.snowflake.net/manuals/sql-reference/sql/create-external-table.html #}
{# This assumes you have already created an external stage #}

{% set ddl %}
    create or replace external table {{source(source_node.source_name, source_node.name)}}
    {%- if columns or partitions or infer_schema -%}
    (
        {%- if partitions -%}{%- for partition in partitions %}
            {{partition.name}} {{partition.data_type}} as {{partition.expression}}{{- ',' if not loop.last or columns|length > 0 or infer_schema -}}
        {%- endfor -%}{%- endif -%}
        {%- if not infer_schema -%}
            {%- for column in columns %}
                {%- set column_quoted = adapter.quote(column.name) if column.quote else column.name %}
                {%- set column_alias -%}
                    {%- if 'alias' in column and column.quote -%}
                        {{adapter.quote(column.alias)}}
                    {%- elif 'alias' in column -%}
                        {{column.alias}}
                    {%- else -%}
                        {{column_quoted}}
                    {%- endif -%}
                {%- endset %}
                {%- set col_expression -%}
                    {%- if column.expression -%}
                        {{column.expression}}
                    {%- else -%}
                        {%- if ignore_case -%}
                        {%- set col_id = 'value:c' ~ loop.index if is_csv else 'GET_IGNORE_CASE($1, ' ~ "'"~ column_quoted ~"'"~ ')' -%}
                        {%- else -%}
                        {%- set col_id = 'value:c' ~ loop.index if is_csv else 'value:' ~ column_quoted -%}
                        {%- endif -%}
                        (case when is_null_value({{col_id}}) or lower({{col_id}}) = 'null' then null else {{col_id}} end)
                    {%- endif -%}
                {%- endset %}
                {{column_alias}} {{column.data_type}} as ({{col_expression}}::{{column.data_type}})
                {{- ',' if not loop.last -}}
            {% endfor %}
        {% else %}
        {%- for column in columns_infer %}
                {%- set col_expression -%}
                {%- if ignore_case -%}
                    {%- set col_id = 'GET_IGNORE_CASE($1, ' ~ "'"~ column[0] ~"'"~ ')' -%}
                {%- else -%}
                    {%- set col_id = 'value:' ~ column[0] -%}
                {%- endif -%}
                    (case when is_null_value({{col_id}}) or lower({{col_id}}) = 'null' then null else {{col_id}} end)
                {%- endset %}
                {{column[0]}} {{column[1]}} as ({{col_expression}}::{{column[1]}})
                {{- ',' if not loop.last -}}
            {% endfor %}
        {%- endif -%}
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
{# {{ log('ddl: ' ~ ddl, info=True) }} #}
{{ ddl }};
{% endmacro %}