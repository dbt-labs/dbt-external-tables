{% macro snowflake_create_empty_table(source_node) %}

    {%- set columns = source_node.columns.values() %}
    {%- set table_name = source_node.name %}
    {%- set schema = source_node.source_name %}

    create or replace table {{source(source_node.source_name, source_node.name)}}
        using template (
        select array_agg(object_construct(*))
        from table(
        infer_schema(
           location=>'@staging.{{ schema }}.dms_{{ schema }}_production_stage/{{ table_name }}/'
           , file_format=>'staging.{{ schema }}.parquet_format'
           , ignore_case=>true
        )
      ));

      grant evolve schema on table {{source(source_node.source_name, source_node.name)}} to role transformer;
      alter table {{source(source_node.source_name, source_node.name)}} set enable_schema_evolution = true;
      
{% endmacro %}
