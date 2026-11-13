{% macro fabric__create_external_table(source_node) %}

    {%- set columns = source_node.columns.values() -%}
    {%- set external = source_node.external -%}
    {%- set not_null_columns = [] -%}
    {# Parsed columns do not retain schema tests; dbt stores them in graph.nodes. #}
    {% if execute %}
        {% for node in graph.nodes.values() %}
            {% if node.resource_type == 'test'
                and node.test_metadata is defined and node.test_metadata
                and node.test_metadata.name == 'not_null'
                and node.test_metadata.namespace in [none, 'dbt']
                and source_node.unique_id in node.depends_on.nodes
                and node.config.enabled
                and not node.config.where %}
                {% do not_null_columns.append(node.column_name) %}
            {% endif %}
        {% endfor %}
    {% endif %}

    {% if external.ansi_nulls is true -%} SET ANSI_NULLS ON; {%- endif %}
    {% if external.quoted_identifier is true -%} SET QUOTED_IDENTIFIER ON; {%- endif %}

    create external table {{source(source_node.source_name, source_node.name)}} (
        {% for column in columns %}
            {%- set nullity = 'NOT NULL' if column.name in not_null_columns or adapter.quote(column.name) in not_null_columns else 'NULL'-%}
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
