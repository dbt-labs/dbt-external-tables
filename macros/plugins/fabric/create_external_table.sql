{% macro fabric__create_external_table(source_node) %}

    {%- set columns = source_node.columns.values() -%}
    {%- set external = source_node.external.get(target.type, source_node.external) -%}

    {% if external.ansi_nulls is true -%} SET ANSI_NULLS ON; {%- endif %}
    {% if external.quoted_identifier is true -%} SET QUOTED_IDENTIFIER ON; {%- endif %}

    {% if target.type == 'synapse'%}

        create external table {{source(source_node.source_name, source_node.name)}} (
            {% for column in columns %}
                {# TODO set nullity based on schema tests?? #}
                {%- set nullity = 'NOT NULL' if 'not_null' in columns.tests else 'NULL'-%}
                {{adapter.quote(column.name)}} {{column.data_type}} {{nullity}}
                {{- ',' if not loop.last -}}
            {% endfor %}
        )
        WITH (
            {# remove keys that are None (i.e. not defined for a given source) #}
            {%- for key, value in external.items() if value is not none and key not in ['ansi_nulls', 'quoted_identifier'] -%}
                {{key}} = 
                    {%- if key in ["location", "schema_name", "object_name"] -%}
                        '{{value}}'
                    {% elif key in ["data_source","file_format"] -%}
                        [{{value}}]
                    {% else -%}
                        {{value}}
                    {%- endif -%}
                {{- ',' if not loop.last -}}
                {%- endfor -%}
        )

    {% elif target.type == 'fabric'%}

        {# These are ON by default. It is valid to set these off in Fabric DWH, but not in Synapse. #}
        {% if external.ansi_nulls is false -%} SET ANSI_NULLS OFF; {%- endif %}
        {% if external.quoted_identifier is false -%} SET QUOTED_IDENTIFIER OFF; {%- endif %}

        create view {{source(source_node.source_name, source_node.name).include(database=false)}} as
        select
        {%- for column in columns %}
            {{adapter.quote(column.name)}}
            {{- ',' if not loop.last -}}
        {% endfor %}
        from
        openrowset
        (
            {#- https://learn.microsoft.com/en-us/sql/t-sql/functions/openrowset-bulk-transact-sql?view=fabric #}
            {#- BULK 'data_file_path', #}
            {#- BULK '{{external.location}}{{'' if external.location.endswith('/') else '/'}}{{external.file_mask}}', -#}
            BULK '{{external.location}}',
            {%- for key, value in external.items() if value is not none and key not in ['location', 'use_column_ordinal', 'file_mask'] %}
            {{key}} = 
                {%- if key in ['HEADER_ROW', 'MAXERRORS', 'FIRSTROW', 'LASTROW', 'ROWS_PER_BATCH'] -%}
                {#%- if key in ["schema_name", "object_name", "FIELDTERMINATOR", "FIELDQUOTE", "ESCAPECHAR", "FORMAT", 'DATA_SOURCE','file_format'] -%#}
                    {{- value}}
                {# perhaps an options will require [] quoting?
                {%- elif key in ['POSSIBLE_QUOTING_NEEDED'] -%}
                    [{{value}}]
                #}
                {%- else -%}
                    '{{value}}'
                {%- endif -%}
            {{- ',' if not loop.last -}}
            {%- endfor %}
        ) with (
        {%- for column in columns %}
            {% if external.use_column_ordinal is true -%}
            {{adapter.quote(column.name)}} {{column.data_type}} {{loop.index}}{# {{nullity}} #}
        {%- else -%}
            {{adapter.quote(column.name)}} {{column.data_type}} {# {{nullity}} #}
        {%- endif -%}
            {{- ',' if not loop.last -}}
        {% endfor %}
        )

    {% endif %}


{% endmacro %}
