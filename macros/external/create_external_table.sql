{% macro create_external_table(source) %}
    {{ adapter_macro('create_external_table', source) }}
{% endmacro %}

{% macro default__create_external_table(source) %}
    {{ exceptions.raise_compiler_error("External table creation is not implemented for the default adapter") }}
{% endmacro %}

{% macro redshift__create_external_table(source) %}

    {% set columns = source.columns %}
    {% set external = source.external %}
    {% set partitions = external.partitions %}

{# https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_EXTERNAL_TABLE.html #}
{# This assumes you have already created an external schema #}

    create external table {{source.database}}.{{source.schema}}.{{source.name}} (
        {% for column in columns.values() %}
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

{% macro spark__create_external_table(source) %}

    {% set columns = source.columns %}
    {% set external = source.external %}
    {% set partitions = external.partition %}

{# https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html #}
    create external table {{source.database}}.{{source.schema}}.{{source.identifier}} (
        {% for column in source.columns.values() %}
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

{% macro snowflake__create_external_table(source) %}

    {%- set external = source.external -%}
    {%- set partitions = external.partitions -%}

{# https://docs.snowflake.net/manuals/sql-reference/sql/create-external-table.html #}
{# This assumes you have already created an external stage #}
    create or replace external table {{source.database}}.{{source.schema}}.{{source.identifier}} (
        {%- if partitions -%}{%- for partition in partitions %}
            {{partition.name}} {{partition.data_type}} as {{partition.expression}},
        {%- endfor -%}{%- endif -%}
        {% for column in source.columns.values() %}
            {{column.name}} {{column.data_type}} as (nullif(value:{{column.name}},'')::{{column.data_type}})
            {{- ',' if not loop.last -}}
        {% endfor %}
    )
    {% if partitions -%} partition by ({{partitions|map(attribute='name')|join(', ')}}) {%- endif %}
    {% if external.location -%} location = {{external.location}} {%- endif %} {# stage #}
    {% if external.auto_refresh -%} auto_refresh = {{external.auto_refresh}} {%- endif %}
    {% if external.file_format -%} file_format = {{external.file_format}} {%- endif %}
{% endmacro %}

{% macro bigquery__create_external_table(source) %}
    {{ exceptions.raise_compiler_error(
        "BigQuery does not support creating external tables in SQL/DDL. 
        Create it from the BQ console.") }}
{% endmacro %}

{% macro presto__create_external_table(source) %}
    {{ exceptions.raise_compiler_error(
        "Presto does not support creating external tables with 
        the Hive connector. Do so from Hive directly.") }}
{% endmacro %}
