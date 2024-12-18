{% macro fabric__dropif(node) %}
    
    {% set ddl %}
      if object_id ('{{source(node.source_name, node.name)}}') is not null
        begin
        drop external table {{source(node.source_name, node.name)}}
        end
    {% endset %}
    
    {{return(ddl)}}

{% endmacro %}
