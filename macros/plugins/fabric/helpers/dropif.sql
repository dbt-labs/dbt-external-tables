{% macro fabric__dropif(source_node) %}
    
    {% set ddl %}
      if object_id ('{{ dbt.escape_single_quotes(this.render()) }}') is not null
        begin
        drop external table {{ this }}
        end
    {% endset %}
    
    {{return(ddl)}}

{% endmacro %}
