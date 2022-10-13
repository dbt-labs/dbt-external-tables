{% macro snowflake_get_copy_sql(source_node, explicit_transaction=false) %}
{# This assumes you have already created an external stage #}

    {%- set columns = source_node.columns.values() -%}
    {%- set external = source_node.external -%}
    {%- set is_csv = dbt_external_tables.is_csv(external.file_format) %}
    {%- set copy_options = external.snowpipe.get('copy_options', none) -%}
   
    {%- if explicit_transaction -%} begin; {%- endif %}
    
    copy into {{source(source_node.source_name, source_node.name)}}
    from {{external.location}} {# stage #}
    file_format = {{external.file_format}}
    match_by_column_name = case_insensitive;
    
    {% if explicit_transaction -%} commit; {%- endif -%}

{% endmacro %}
