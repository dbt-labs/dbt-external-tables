{% macro spark__create_schema(source_node) %}
    {# https://spark.apache.org/docs/latest/sql-ref-syntax-ddl-create-database.html #}

    {% set ddl %}
        create schema if not exists {{ source_node.schema }}
    {% endset %}

    {{return(ddl)}}

{% endmacro %}