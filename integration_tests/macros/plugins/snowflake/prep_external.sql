{% macro snowflake__prep_external() %}

    {% set external_stage = target.schema ~ '.dbt_external_tables_testing' %}
    {% set parquet_file_format = target.schema ~ '.dbt_external_tables_testing_parquet' %}

    {% set create_external_stage_and_file_format %}
    
        begin;
        create or replace stage
            {{ external_stage }}
            url = 's3://dbt-external-tables-testing';
        
        create or replace file format {{ parquet_file_format }} type = parquet;
        commit;
            
    {% endset %}

    {% do log('Creating external stage ' ~ external_stage, info = true) %}
    {% do log('Creating parquet file format ' ~ parquet_file_format, info = true) %}
    {% do run_query(create_external_stage_and_file_format) %}
    
{% endmacro %}
