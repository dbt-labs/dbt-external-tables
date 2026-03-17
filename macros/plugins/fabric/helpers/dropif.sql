{% macro fabric__dropif(node) %}
    
  {% if target.type == 'synapse'%}

    {% set ddl %}
      if object_id ('{{source(node.source_name, node.name)}}') is not null
        begin
        drop external table {{source(node.source_name, node.name)}}
        end
    {% endset %}

  {% elif target.type =='fabric' %}

    {% set ddl %}
      if object_id ('{{source(node.source_name, node.name)}}') is not null
        begin
        drop view {{source(node.source_name, node.name).include(database=false)}}
        end
    {% endset %}

  {% endif %}
    
  {{return(ddl)}}

{% endmacro %}
