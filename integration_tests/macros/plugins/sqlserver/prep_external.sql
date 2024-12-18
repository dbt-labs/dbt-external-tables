{% macro fabric__prep_external() %}

    {% set external_data_source = target.schema ~ '.dbt_external_tables_testing' %}

    {% if target.type == "synapse"%}

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

    {% elif target.type == "sqlserver" %}

        {% set cred_name = 'synapse_reader' %}

        {% set create_database_scoped_credential %}
            IF NOT EXISTS ( SELECT * FROM sys.database_scoped_credentials WHERE name = '{{ cred_name }}')
                CREATE DATABASE SCOPED CREDENTIAL [{{ cred_name }}] WITH
                    IDENTITY = '{{ env_var("DBT_SYNAPSE_UID") }}',
                    SECRET = '{{ env_var("DBT_SYNAPSE_PWD") }}'

        {% endset %}

        {% set create_external_data_source %}
            IF NOT EXISTS ( SELECT * FROM sys.external_data_sources WHERE name = '{{external_data_source}}' )

            CREATE EXTERNAL DATA SOURCE [{{external_data_source}}] WITH (
                TYPE = RDBMS,
                LOCATION = '{{ env_var("DBT_SYNAPSE_SERVER") }}',
                DATABASE_NAME = '{{ env_var("DBT_SYNAPSE_DB") }}',
                CREDENTIAL = [{{ cred_name }}]
            )
        {% endset %}

    {%- endif %}
    

    {% if target.type == "sqlserver" -%}
        {% do log('Creating database scoped credential ' ~ cred_name, info = true) %}
        {% do run_query(create_database_scoped_credential) %}
    {%- endif %}

    {% do log('Creating external data source ' ~ external_data_source, info = true) %}
    {% do run_query(create_external_data_source) %}

    {% if target.type == "synapse" -%}
        {% do log('Creating external file format ' ~ external_file_format, info = true) %}
        {% do run_query(create_external_file_format) %}
    {%- endif %}

{% endmacro %}
