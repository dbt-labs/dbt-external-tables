{% macro maxcompute__create_external_table(source_node) %}

    {%- set columns = source_node.columns.values() -%}
    {%- set external = source_node.external -%}
    {%- set partitions = external.partitions -%}
    {%- set serdeproperties = external.serdeproperties -%}
    {%- set stored_by = external.stored_by -%}
    {%- set stored_as = external.stored_as -%}

    create external table {{source(source_node.source_name, source_node.name)}} (
        {% for column in columns %}
            {{adapter.quote(column.name)}} {{column.data_type}}
            {{- ',' if not loop.last -}}
        {% endfor %}
    )

    {% if partitions -%} 
    partitioned by (
        {%- for partition in partitions -%}
            {{adapter.quote(partition.name)}} {{partition.data_type}}{{', ' if not loop.last}}
        {%- endfor -%}
    ) 
    {%- endif %}
    {% if stored_by -%}
    stored by '{{stored_by}}'
    {%- endif %}
    {% if stored_as -%}
    stored as {{stored_as}}
    {%- endif %}
    {% if serdeproperties %}
    with serdeproperties (
        {%- for key, value in serdeproperties.items() -%}
            '{{key}}' = '{{value}}'{{', ' if not loop.last}}
        {%- endfor -%}
    )
    {%- endif %}
    location '{{external.location}}/'

{% endmacro %}
