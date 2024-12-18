{% macro fabric__create_external_schema(source_node) %}
    {# https://learn.microsoft.com/en-us/sql/t-sql/statements/create-schema-transact-sql?view=sql-server-ver16 #}

    {% set ddl %}
        IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{{ source_node.schema }}')
        BEGIN
        EXEC('CREATE SCHEMA [{{ source_node.schema }}]')
        END 
    {% endset %}

    {{return(ddl)}}

{% endmacro %}
