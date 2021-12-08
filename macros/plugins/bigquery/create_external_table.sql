{% macro bigquery__create_external_table(source_node) %}

    {%- set columns = source_node.columns.values() -%}
    {%- set external = source_node.external -%}
    {%- set partitions = external.partitions -%}
    {%- set options = external.options -%}
    
    {%- set uris = [] -%}
    {%- if options is mapping and options.get('uris', none) -%}
        {%- set uris = external.options.get('uris') -%}
    {%- else -%}
        {%- set uris = [external.location] -%}
    {%- endif -%}

    create or replace external table {{source(source_node.source_name, source_node.name)}}
        {%- if columns -%}(
            {% for column in columns %}
                {{column.name}} {{column.data_type}} {{- ',' if not loop.last -}}
            {%- endfor -%}
        )
        {% endif %}
        {% if options and options.get('hive_partition_uri_prefix', none) %}
        with partition columns {%- if partitions %} (
            {%- for partition in partitions %}
                {{partition.name}} {{partition.data_type}}{{',' if not loop.last}}
            {%- endfor -%}
        ) {% endif -%}
        {% endif %}
        options (
            uris = [{%- for uri in uris -%} '{{uri}}' {{- "," if not loop.last}} {%- endfor -%}]
            {%- if options is mapping -%}
            {%- for key, value in options.items() if key != 'uris' %}
                {%- if value is string -%}
                , {{key}} = '{{value}}'
                {%- else -%}
                , {{key}} = {{value}}
                {%- endif -%}
            {%- endfor -%}
            {%- endif -%}
        )
{% endmacro %}
