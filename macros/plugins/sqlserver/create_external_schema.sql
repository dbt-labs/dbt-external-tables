{% macro sqlserver__create_external_schema(source_node) %}
    {# https://spark.apache.org/docs/latest/sql-ref-syntax-ddl-create-database.html #}

    {% set ddl %}
        IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{{ source_node.schema }}')
        BEGIN
        EXEC('CREATE SCHEMA [{{ source_node.schema }}]')
        END 
    {% endset %}

    {{return(ddl)}}

{% endmacro %}
