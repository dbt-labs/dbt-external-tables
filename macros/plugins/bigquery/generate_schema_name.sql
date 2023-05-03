{% macro bigquery__generate_schema_name(source_node) %}
    {%- set fqn -%}
        {%- if source_node.database -%}
            {{ adapter.quote(source_node.database) }}.{{ source_node.schema }}
        {%- else -%}
            {{ source_node.schema }}
        {%- endif -%}
    {%- endset -%}

    {{ return(fqn) }}
{% endmacro %}
