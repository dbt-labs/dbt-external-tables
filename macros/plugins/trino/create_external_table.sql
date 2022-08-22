{% macro trino__create_external_table(source_node) %}
{%- set columns = source_node.columns.values() -%}
{%- set external = source_node.external -%}

create table {{source(source_node.source_name, source_node.name)}} (

    {% for column in columns %}
        {{column.name}} {{column.data_type}} {{- ',' if not loop.last -}}
    {% endfor %}
    {%- if external.partitions %} {{- ',' }}
        {% for partition in external.partitions -%}
            {{ partition.name }} {{ partition.data_type }} {{- ',' if not loop.last }}
        {% endfor %}
    {%- endif %}

)
{% if external.comment -%} comment '{{external.comment}}' {%- endif %}
with (

    external_location = '{{external.location}}'

    {%- if external.file_format %} {{- ',' }}
    format = '{{external.file_format}}'
    {%- endif -%}

    {%- if external.partitions %} {{- ',' }}
    partitioned_by = ARRAY[
        {%- for partition in external.partitions -%}
            '{{ partition.name }}' {{- ', ' if not loop.last }}
        {%- endfor -%}
    ]
    {%- endif -%}

    {%- if external.table_properties %} {{- ',' }}
    {% for name, value in external.table_properties.items() -%}
        {{ name }}={{ '{!r}'.format(value) }} {{- ',' if not loop.last }}
    {% endfor -%}
    {%- endif %}

)
{% endmacro %}
