{% macro create_snowpipe(source_node) %}

    {%- set columns = source_node.columns.values() -%}
    {%- set external = source_node.external -%}
    {%- set partitions = external.partitions -%}
    
    {%- set is_csv = dbt_external_tables.is_csv(external.file_format) -%}

    create or replace table {{source(source_node.source_name, source_node.name)}} (
        {%- for column in columns %}
            {{column.name}} {{column.data_type}}{{- ',' if not loop.last -}}
        {% endfor %}
    );

{# https://docs.snowflake.com/en/sql-reference/sql/create-pipe.html #}
{# This assumes you have already created an external stage #}
    create or replace pipe {{source(source_node.source_name, source_node.name)}}
        {% if external.auto_refresh -%} auto_refresh = {{external.auto_refresh}} {%- endif %}
        {% if external.aws_sns_topic -%} aws_sns_topic = {{external.aws_sns_topic}} {%- endif %}
        {% if external.integration -%} integration = '{{external.integration}}' {%- endif %}
        as 
        copy into {{source(source_node.source_name, source_node.name)}}
        from (
            select
            {%- for column in columns %}
                {%- set col_expression -%}
                    {%- if is_csv -%}nullif(value:c{{loop.index}},''){# special case: get columns by ordinal position #}
                    {%- else -%}nullif(value:{{column.name}},''){# standard behavior: get columns by name #}
                    {%- endif -%}
                {%- endset %}
                {{column.name}} {{column.data_type}} as ({{col_expression}}::{{column.data_type}})
                {{- ',' if not loop.last -}}
            {% endfor %}
            from {{external.location}} {# stage #}
        )
        file_format = {{external.file_format}}

{% endmacro %}
