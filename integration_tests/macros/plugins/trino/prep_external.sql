{% macro trino__prep_external() %}

    {% set external_schema = target.schema %}
    {% set external_schema_location = 's3://test_bucket/external_schema.db' %}

    {% set create_external_schema %}
    
        create schema if not exists
            hive.{{ external_schema }}
        with (
            location = {{ external_schema_location }}
        )
            
    {% endset %}
    
    {% do log('Creating external schema ' ~ external_schema, info = true) %}
    {% do run_query(create_external_schema) %}
    
{% endmacro %}
