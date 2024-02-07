{%- macro bigquery__create_external_schema(source_node) -%}
    {%- set fqn -%}
        {%- if source_node.database -%}
            `{{ source_node.database }}`.{{ source_node.schema }}
        {%- else -%}
            {{ source_node.schema }}
        {%- endif -%}
    {%- endset -%}

    {% set schema_exists_query %}
        select * from `{{ source_node.database }}`.INFORMATION_SCHEMA.SCHEMATA where schema_name = '{{ source_node.schema }}' limit 1
    {% endset %}
    {% if execute %}
        {% set schema_exists = run_query(schema_exists_query)|length > 0 %}
    {% else %}
        {% set schema_exists = false %}
    {% endif %}  

    {%- if not schema_exists -%}
        {%- set ddl -%}
            create schema if not exists {{ fqn }}
        {%- endset -%}
        {{ return(ddl) }}
    {%- else -%}
        {{ return('') }}
    {% endif %} 
{%- endmacro -%}
