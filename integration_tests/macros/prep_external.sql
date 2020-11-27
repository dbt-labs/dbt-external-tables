{% macro prep_external() %}
    {{ return(adapter.dispatch('prep_external', dbt_external_tables._get_dbt_external_tables_namespaces())()) }}
{% endmacro %}

{% macro default__prep_external() %}
    {% do log('No prep necessary, skipping', info = true) %}
    {# noop #}
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

{% macro sqlserver__prep_external() %}

    {% set external_data_source = target.schema ~ '.dbt_external_tables_testing' %}
    
    {% set create_external_data_source %}
        CREATE EXTERNAL DATA SOURCE [{{external_data_source}}] WITH (
            TYPE = HADOOP,
            LOCATION = N'abfss://dbt-external-tables-testing@dbtsynapselake.blob.core.windows.net'
        )
    {% endset %}

    {% set external_file_format = target.schema ~ '.dbt_external_ff_testing' %}

    {% set create_external_file_format %}
        CREATE EXTERNAL FILE FORMAT [{{external_file_format}}] 
        WITH (
            FORMAT_TYPE = DELIMITEDTEXT, 
            FORMAT_OPTIONS (
                FIELD_TERMINATOR = N',', 
                FIRST_ROW = 2, 
                USE_TYPE_DEFAULT = True
            )
        )
    {% endset %}
    
    {% do log('Creating external data source ' ~ external_data_source, info = true) %}
    {% do run_query(create_external_data_source) %}
    {% do log('Creating external file format ' ~ external_file_format, info = true) %}
    {% do run_query(create_external_file_format) %}
    
{% endmacro %}
