{% macro fabric__dropif(source_node, relation) %}
    
    {% set ddl %}
      if object_id ('{{ dbt.escape_single_quotes(relation.render()) }}') is not null
        begin
        drop external table {{ relation }}
        end
    {% endset %}
    
    {{return(ddl)}}

{% endmacro %}
