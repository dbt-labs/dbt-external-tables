{% macro snowflake_create_empty_table(source_node) %}

    {%- set columns = source_node.columns.values() -%}

    create or replace table {{source(source_node.source_name, source_node.name)}} (
        {%- for column in columns %}
            {{column.name}} {{column.data_type}},
        {% endfor %}
            _dbt_copied_at timestamp
    );

{% endmacro %}

{% macro snowflake_get_copy_sql(source_node) %}
{# This assumes you have already created an external stage #}

    {%- set columns = source_node.columns.values() -%}
    {%- set external = source_node.external -%}
    {%- set is_csv = dbt_external_tables.is_csv(external.file_format) -%}
    
    copy into {{source(source_node.source_name, source_node.name)}}
    from (
        select
        {%- for column in columns %}
            {%- set col_expression -%}
                {%- if is_csv -%}nullif(${{loop.index}},''){# special case: get columns by ordinal position #}
                {%- else -%}nullif($1:{{column.name}},''){# standard behavior: get columns by name #}
                {%- endif -%}
            {%- endset %}
            {{col_expression}}::{{column.data_type}} as {{column.name}},
        {% endfor %}
            current_timestamp::timestamp as _dbt_copied_at
        from {{external.location}} {# stage #}
    )
    file_format = {{external.file_format}}

{% endmacro %}

{% macro snowflake_create_snowpipe(source_node) %}

    {%- set external = source_node.external -%}

{# https://docs.snowflake.com/en/sql-reference/sql/create-pipe.html #}
    create or replace pipe {{source(source_node.source_name, source_node.name)}}
        {% if external.auto_ingest -%} auto_ingest = {{external.auto_ingest}} {%- endif %}
        {% if external.aws_sns_topic -%} aws_sns_topic = {{external.aws_sns_topic}} {%- endif %}
        {% if external.integration -%} integration = '{{external.integration}}' {%- endif %}
        as {{ dbt_external_tables.snowflake_get_copy_sql(source_node) }}

{% endmacro %}
