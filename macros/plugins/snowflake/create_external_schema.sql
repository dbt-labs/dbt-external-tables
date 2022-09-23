{% macro snowflake__create_external_schema(source_node) %}
    {# https://docs.snowflake.com/en/sql-reference/sql/create-schema.html #}

    {% set ddl %}
        create schema if not exists {{ source_node.schema }}
    {% endset %}

    {{return(ddl)}}

{% endmacro %}