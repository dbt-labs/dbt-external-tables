{%- macro generate_schema_name(source_node) -%}
    {{ adapter.dispatch('generate_schema_name', 'dbt_external_tables')(source_node) }}
{%- endmacro -%}

{%- macro default__generate_schema_name(source_node) -%}
    {%- set fqn -%}
        {%- if source_node.database -%}
            {{ source_node.database }}.{{ source_node.schema }}
        {%- else -%}
            {{ source_node.schema }}
        {%- endif -%}
    {%- endset -%}

    {{ return(fqn) }}
{%- endmacro -%}
