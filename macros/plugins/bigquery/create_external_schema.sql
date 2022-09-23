{% macro bigquery__create_external_schema(source_node) %}
    {# https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_schema_statement #}

    {% set ddl %}
        create schema if not exists {{ source_node.schema }}
    {% endset %}

    {{return(ddl)}}

{% endmacro %}