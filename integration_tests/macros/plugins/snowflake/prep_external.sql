{% macro snowflake__prep_external() %}

    {% set external_stage = target.schema ~ '.dbt_external_tables_testing' %}

    {% set create_external_stage %}

        create or replace stage
            {{ external_stage }}
            url = 's3://dbt-external-tables-testing';
            
    {% endset %}

    {% do log('Creating external stage ' ~ external_stage, info = true) %}
    {% do run_query(create_external_stage) %}
    
    {% set set_autocommit_false %}
        alter user {{ target.user }} set autocommit = false;
    {% endset %}

    {% do log('Turning off autocommit for user ' ~ target.user, info = true) %}
    {% do run_query(set_autocommit_false) %}
    
{% endmacro %}
