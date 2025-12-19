{%- macro create_struct_type_definition(fields) -%}
    STRUCT<
    {%for field in fields%}{{get_column_definition(field)}}{{- ',' if not loop.last -}}{%- endfor %}
    >
{%- endmacro -%}

{%- macro get_column_definition(column) -%}
    {%- set column_quoted = adapter.quote(column.name) if column.quote else column.name %}
    {%- set column_type = create_struct_type_definition(column.fields) if column.data_type == 'STRUCT' else column.data_type %}
    {%- set column_type = 'ARRAY<' ~ column_type ~ '>' if column.mode == 'REPEATED' else column_type %}
    {{column_quoted}} {{column_type}} {{- ' NOT NULL' if column.mode == 'REQUIRED'}}
{%- endmacro -%}

{% macro bigquery__create_external_table(source_node) %}
    {%- set columns = source_node.columns.values() -%}
    {%- set external = source_node.external -%}
    {%- set partitions = external.partitions -%}
    {%- set options = external.options -%}
    {%- set non_string_options = ['max_staleness'] %}

    {% if options is mapping and options.get('connection_name', none) %}
        {% set connection_name = options.pop('connection_name') %}
    {% endif %}
    
    {%- set uris = [] -%}
    {%- if options is mapping and options.get('uris', none) -%}
        {%- set uris = external.options.get('uris') -%}
    {%- else -%}
        {%- set uris = [external.location] -%}
    {%- endif -%}

    create or replace external table {{source(source_node.source_name, source_node.name)}}
        {%- if columns -%}(
            {% for column in columns %}
                {{ get_column_definition(column) }} {{- ',' if not loop.last -}}
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
        {% if connection_name %}
            with connection `{{ connection_name }}`
        {% endif %}
        options (
            uris = [{%- for uri in uris -%} '{{uri}}' {{- "," if not loop.last}} {%- endfor -%}]
            {%- if options is mapping -%}
            {%- for key, value in options.items() if key != 'uris' %}
                {%- if value is string and key not in non_string_options -%}
                , {{key}} = '{{value}}'
                {%- else -%}
                , {{key}} = {{value}}
                {%- endif -%}
            {%- endfor -%}
            {%- endif -%}
        )
{% endmacro %}
