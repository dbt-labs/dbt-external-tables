{% macro refresh_external_table(source) %}
    {{ adapter_macro('refresh_external_table', source) }}
{% endmacro %}

{% macro default__refresh_external_table(source) %}
    {{ exceptions.raise_compiler_error("External table creation is not implemented for the default adapter") }}
{% endmacro %}

{% macro redshift__refresh_external_table(source) %}

    {%- set partitions = source.external.get('partitions',[]) -%}
    {%- set finals = generate_hive_partitions(partitions) -%}
    
    {%- set ddl -%}

    {{ redshift__alter_table_add_partitions(
        source.database ~ "." ~ source.schema ~ "." ~ source.identifier,
        source.external.location,
        finals
      )
    }}

    {%- endset -%}
    
    {{return(ddl)}}
    
{% endmacro %}

{% macro snowflake__refresh_external_table(source) %}

    {% set alter %}
    alter external table {{source.database}}.{{source.schema}}.{{source.identifier}} refresh
    {% endset %}
    
    {{return(alter)}}
    
{% endmacro %}

{% macro spark__refresh_external_table(source) %}

    {% if source.external.hive %}
    
        {% set alter %}
        alter table {{source.database}}.{{source.schema}}.{{source.identifier}} add
        {% for partition in partitions %}

            partition ({%- for part in partition.partition_by -%}{{ part.name }}='{{ part.value }}'{{',' if not loop.last}}{%- endfor -%})
            location '{{ source_external_location }}{{ partition.path }}/' {{',' if not loop.last}}

        {% endfor %}
        {% endset %}
    
    {% else %}
        {% set alter = '--noop' %}
    {% endif %}
    
    {{return(alter)}}
    
{% endmacro %}

{% macro bigquery__refresh_external_table(source) %}
    {{ exceptions.raise_compiler_error(
        "BigQuery does not support creating external tables in SQL/DDL. 
        Create it from the BQ console.") }}
{% endmacro %}

{% macro presto__refresh_external_table(source) %}
    {{ exceptions.raise_compiler_error(
        "Presto does not support creating external tables with 
        the Hive connector. Do so from Hive directly.") }}
{% endmacro %}
