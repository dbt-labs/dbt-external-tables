{% macro sqlserver__dropif(node) %}
    
    {% set ddl %}
      if object_id ('{{source(node.source_name, node.name)}}') is not null
        begin
        drop external table {{source(node.source_name, node.name)}}
        end
    {% endset %}
    
    {{return(ddl)}}

{% endmacro %}

{% macro synapse__dropif(node) %}
    {% do return( dbt_external_tables.sqlserver__dropif(node)) %}
{% endmacro %}
