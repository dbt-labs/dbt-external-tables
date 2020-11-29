{% macro sqlserver__prep_external() %}

    {% set external_data_source = target.schema ~ '.dbt_external_tables_testing' %}
    
    {% set create_external_data_source %}
        IF NOT EXISTS ( SELECT * FROM sys.external_data_sources WHERE name = '{{external_data_source}}' )

        CREATE EXTERNAL DATA SOURCE [{{external_data_source}}] WITH (
            TYPE = HADOOP,
            LOCATION = 'wasbs://dbt-external-tables-testing@dbtsynapselake.blob.core.windows.net'
        )
    {% endset %}

    {% set external_file_format = target.schema ~ '.dbt_external_ff_testing' %}

    {% set create_external_file_format %}
        IF NOT EXISTS ( SELECT * FROM sys.external_file_formats WHERE name = '{{external_file_format}}' )

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
