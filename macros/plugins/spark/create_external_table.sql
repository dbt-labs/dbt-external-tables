{% macro spark__create_external_table(source_node) %}

    {%- set columns = source_node.columns.values() -%}
    {%- set external = source_node.external -%}
    {%- set partitions = external.partitions -%}
    {%- set options = external.options -%}

{# https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html #}
    create table {{source(source_node.source_name, source_node.name)}} 
    {%- if columns|length > 0 %} (
        {% for column in columns %}
            {{column.name}} {{column.data_type}}
            {{- ',' if not loop.last -}}
        {% endfor %}
    ) {% endif -%}
    {% if external.using %} using {{external.using}} {%- endif %}
    {% if options -%} options (
        {%- for key, value in options.items() -%}
            '{{ key }}' = '{{value}}' {{- ', \n' if not loop.last -}}
        {%- endfor -%}
    ) {%- endif %}
    {% if partitions -%} partitioned by (
        {%- for partition in partitions -%}
            {{partition.name}} {{partition.data_type}}{{', ' if not loop.last}}
        {%- endfor -%}
    ) {%- endif %}
    {% if external.row_format -%} row format {{external.row_format}} {%- endif %}
    {% if external.file_format -%} stored as {{external.file_format}} {%- endif %}
    {% if external.location -%} location '{{external.location}}' {%- endif %}
    {% if external.table_properties -%} tblproperties {{ external.table_properties }} {%- endif -%}

{% endmacro %}
