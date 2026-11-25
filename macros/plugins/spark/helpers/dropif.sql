{% macro spark__dropif(source_node) %}
    
    {% set ddl %}
        drop table if exists {{ this }}
    {% endset %}
    
    {{return(ddl)}}

{% endmacro %}
