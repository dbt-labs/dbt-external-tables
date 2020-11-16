{% macro prep_external() %}
    {{ return(adapter.dispatch('prep_external', dbt_external_tables._get_dbt_external_tables_namespaces())()) }}
{% endmacro %}

{% macro redshift__prep_external() %}

    {% set external_schema = target.schema ~ '_spectrum' %}
    
    {% set create_external_schema %}
    
        create external schema if not exists
            {{ external_schema }}
            from data catalog
            database '{{ external_schema }}'
            iam_role '{{ env_var("SPECTRUM_IAM_ROLE", "") }}'
            create external database if not exists;
            
    {% endset %}
    
    {% do log('Creating external schema ' ~ external_schema, info = true) %}
    {% do run_query(create_external_schema) %}
    
{% endmacro %}

{% macro snowflake__prep_external() %}

    {% set external_stage = target.schema ~ '.dbt_external_tables_testing' %}

    {% set create_external_stage %}

        create or replace stage
            {{ external_stage }}
            url = 's3://dbt-external-tables-testing';
            
    {% endset %}

    {% do log('Creating external stage ' ~ external_stage, info = true) %}
    {% do run_query(create_external_stage) %}
    
{% endmacro %}
