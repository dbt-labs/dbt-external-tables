{% macro redshift__dropif(source_node) %}
    
    {% set ddl %}
        drop table if exists {{ this }} cascade
    {% endset %}
    
    {{return(ddl)}}

{% endmacro %}
