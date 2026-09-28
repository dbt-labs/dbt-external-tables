{%- macro bigquery__create_external_schema(source_node) -%}
    {%- set fqn -%}
        {%- if source_node.database -%}
            `{{ source_node.database }}`.{{ source_node.schema }}
        {%- else -%}
            {{ source_node.schema }}
        {%- endif -%}
    {%- endset -%}

    {{ return('create schema if not exists ' ~ fqn) }}

{%- endmacro -%}
