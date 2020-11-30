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
