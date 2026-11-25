{% macro redshift__dropif(source_node, relation) %}
    
    {% set ddl %}
        drop table if exists {{ relation }} cascade
    {% endset %}
    
    {{return(ddl)}}

{% endmacro %}
