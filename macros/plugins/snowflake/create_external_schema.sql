{% macro snowflake__create_external_schema(source_node) %}

    {% set schema_exists_query %}
        show terse schemas like '{{ source_node.schema }}' in database {{ source_node.database }} limit 1;
    {% endset %}
    {% if execute %}
        {% set schema_exists = run_query(schema_exists_query)|length > 0 %}
    {% else %}
        {% set schema_exists = false %}
    {% endif %}    

    {% if schema_exists %}
        {% set ddl %}
            select 'Schema {{ source_node.schema }} exists' from dual;
        {% endset %}
    {% else %}
        {% set fqn %}
            {% if source_node.database %}
                {{ source_node.database }}.{{ source_node.schema }}
            {% else %}
                {{ source_node.schema }}
            {% endif %}
        {% endset %}

        {% set ddl %}
            create schema if not exists {{ fqn }};
        {% endset %}
    {% endif %}

    {% do return(ddl) %}

{% endmacro %}
