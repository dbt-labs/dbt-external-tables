{%- macro create_external_schema(source_node) -%}
    {{ adapter.dispatch('create_external_schema', 'dbt_external_tables')(source_node) }}
{%- endmacro -%}

{%- macro default__create_external_schema(source_node) -%}
    {%- set fqn -%}
        {%- if source_node.database -%}
            {{ source_node.database }}.{{ source_node.schema }}
        {%- else -%}
            {{ source_node.schema }}
        {%- endif -%}
    {%- endset -%}

    {%- set ddl -%}
        create schema if not exists {{ fqn }}
    {%- endset -%}

    {{ return(ddl) }}
{%- endmacro -%}
