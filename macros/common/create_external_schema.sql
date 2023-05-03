{%- macro create_external_schema(source_node) -%}
    {{ adapter.dispatch('create_external_schema', 'dbt_external_tables')(source_node) }}
{%- endmacro -%}

{%- macro default__create_external_schema(source_node) -%}
    {%- set ddl -%}
        create schema if not exists {{ dbt_external_tables.generate_schema_name(source_node) }}
    {%- endset -%}

    {{ return(ddl) }}
{%- endmacro -%}
