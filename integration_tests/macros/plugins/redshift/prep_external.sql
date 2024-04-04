{% macro redshift__prep_external() %}

    {% set external_schema = target.schema ~ '_spectrum' %}
    
    {% set create_external_schema %}
    
        create external schema if not exists
            {{ external_schema }}
            from data catalog
            database '{{ external_schema }}'
            iam_role '{{ env_var("REDSHIFT_SPECTRUM_IAM_ROLE") }}'
            create external database if not exists;
            
    {% endset %}
    
    {% do log('Creating external schema ' ~ external_schema, info = true) %}
    {% do run_query(create_external_schema) %}
    
{% endmacro %}
