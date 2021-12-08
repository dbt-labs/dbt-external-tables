{% macro snowflake__prep_external() %}

    {% set external_stage = target.schema ~ '.dbt_external_tables_testing' %}

    {% set create_external_stage %}
    
        begin;
        create or replace stage
            {{ external_stage }}
            url = 's3://dbt-external-tables-testing';
        commit;
            
    {% endset %}

    {% do log('Creating external stage ' ~ external_stage, info = true) %}
    {% do run_query(create_external_stage) %}
    
{% endmacro %}
