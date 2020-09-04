{% macro create_external_table(source_node) %}
    {{ adapter.dispatch('create_external_table', 
        packages = dbt_external_tables._get_dbt_external_tables_namespaces()) 
        (source_node) }}
{% endmacro %}

{% macro default__create_external_table(source_node) %}
    {{ exceptions.raise_compiler_error("External table creation is not implemented for the default adapter") }}
{% endmacro %}

{% macro redshift__create_external_table(source_node) %}

    {%- set columns = source_node.columns.values() -%}
    {%- set external = source_node.external -%}
    {%- set partitions = external.partitions -%}

{# https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_EXTERNAL_TABLE.html #}
{# This assumes you have already created an external schema #}

    create external table {{source(source_node.source_name, source_node.name)}} (
        {% for column in columns %}
            {{adapter.quote(column.name)}} {{column.data_type}}
            {{- ',' if not loop.last -}}
        {% endfor %}
    )
    {% if partitions -%} partitioned by (
        {%- for partition in partitions -%}
            {{adapter.quote(partition.name)}} {{partition.data_type}}{{', ' if not loop.last}}
        {%- endfor -%}
    ) {%- endif %}
    {% if external.row_format -%} row format {{external.row_format}} {%- endif %}
    {% if external.file_format -%} stored as {{external.file_format}} {%- endif %}
    {% if external.location -%} location '{{external.location}}' {%- endif %}
    {% if external.table_properties -%} table properties {{external.table_properties}} {%- endif %}

{% endmacro %}

{% macro spark__create_external_table(source_node) %}

    {%- set columns = source_node.columns.values() -%}
    {%- set external = source_node.external -%}
    {%- set partitions = external.partition -%}

{# https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html #}
    create external table {{source(source_node.source_name, source_node.name)}} (
        {% for column in columns %}
            {{column.name}} {{column.data_type}}
            {{- ',' if not loop.last -}}
        {% endfor %}
    )
    {% if partitions -%} partitioned by (
        {%- for partition in partitions -%}
            {{partition.name}} {{partition.data_type}}{{', ' if not loop.last}}
        {%- endfor -%}
    ) {%- endif %}
    {% if external.row_format -%} row format {{external.row_format}} {%- endif %}
    {% if external.file_format -%} stored as {{external.file_format}} {%- endif %}
    {% if external.location -%} location '{{external.location}}' {%- endif %}
    {% if external.table_properties -%} tbl_properties {{external.table_properties}} {%- endif %}
{% endmacro %}

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
            {{partition.name}} {{partition.data_type}} as {{partition.expression}}{{- ',' if columns|length > 0 -}}
        {%- endfor -%}{%- endif -%}
        {%- for column in columns %}
            {%- set col_expression -%}
                {%- if is_csv -%}nullif(value:c{{loop.index}},''){# special case: get columns by ordinal position #}
                {%- else -%}nullif(value:{{column.name}},''){# standard behavior: get columns by name #}
                {%- endif -%}
            {%- endset %}
            {{column.name}} {{column.data_type}} as ({{col_expression}}::{{column.data_type}})
            {{- ',' if not loop.last -}}
        {% endfor %}
    )
    {%- endif -%}
    {% if partitions %} partition by ({{partitions|map(attribute='name')|join(', ')}}) {% endif %}
    location = {{external.location}} {# stage #}
    {% if external.auto_refresh -%} auto_refresh = {{external.auto_refresh}} {%- endif %}
    {% if external.pattern -%} pattern = '{{external.pattern}}' {%- endif %}
    file_format = {{external.file_format}}
{% endmacro %}

{% macro bigquery__create_external_table(source_node) %}
    {{ exceptions.raise_compiler_error(
        "BigQuery does not support creating external tables in SQL/DDL.
        Create it from the BQ console.") }}
{% endmacro %}

{% macro presto__create_external_table(source_node) %}
    {{ exceptions.raise_compiler_error(
        "Presto does not support creating external tables with
        the Hive connector. Do so from Hive directly.") }}
{% endmacro %}
