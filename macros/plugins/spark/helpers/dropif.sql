{% macro spark__dropif(source_node, relation) %}
    
    {% set ddl %}
        drop table if exists {{ relation }}
    {% endset %}
    
    {{return(ddl)}}

{% endmacro %}
