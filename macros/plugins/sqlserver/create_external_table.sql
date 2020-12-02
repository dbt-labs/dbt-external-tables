{% macro sqlserver__create_external_table(source_node) %}

    {%- set columns = source_node.columns.values() -%}
    {%- set external = source_node.external -%}

    {% set query %}
        SELECT Type_desc
        FROM [sys].[external_data_sources]
        WHERE Name = '{{external.data_source}}'
    {% endset %}
    {% set results = run_query(query) %}
    {% set datasource_type = results.columns[0].values()[0] %}

    {%- if datasource_type == "HADOOP" -%}
    {% set dict = {'DATA_SOURCE': external.data_source,
                    'LOCATION' : external.location, 
                    'FILE_FORMAT' : external.file_format, 
                    'REJECT_TYPE' : external.reject_type, 
                    'REJECT_VALUE' : external.reject_value} -%}
    {%- elif datasource_type == "RDBMS" -%}
    {% set dict = {'DATA_SOURCE': external.data_source,
                    'SCHEMA_NAME' : external.schema_name, 
                    'OBJECT_NAME' : external.object_name} -%}
    {%- endif %}
    {% if external.ansi_nulls is true -%} SET ANSI_NULLS ON; {%- endif %}
    {% if external.quoted_identifier is true -%} SET QUOTED_IDENTIFIER ON; {%- endif %}

    create external table {{source(source_node.source_name, source_node.name)}} (
        {% for column in columns %}
            {# TODO set nullity based on schema tests?? #}
            {%- set nullity = 'NULL' if 'not_null' in columns.tests else 'NOT NULL'-%}
            {{adapter.quote(column.name)}} {{column.data_type}} {{nullity}}
            {{- ',' if not loop.last -}}
        {% endfor %}
    )
    WITH (
        {%- for key, value in dict.items() %}
            {{key}} = 
                {%- if key in ["LOCATION", "SCHEMA_NAME", "OBJECT_NAME"] -%}
                    '{{value}}'
                {%- elif key in ["DATA_SOURCE","FILE_FORMAT"] -%}
                    [{{value}}]
                {%- else -%}
                    {{value}}
                {%- endif -%}
            {{- ',' if not loop.last -}}
            {%- endfor -%}
    )
{% endmacro %}
