{% macro fabric__create_external_table(source_node) %}

    {%- set columns = source_node.columns.values() -%}
    {%- set external = source_node.external -%}

    {% if external.ansi_nulls is true -%} SET ANSI_NULLS ON; {%- endif %}
    {% if external.quoted_identifier is true -%} SET QUOTED_IDENTIFIER ON; {%- endif %}

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
{% endmacro %}
