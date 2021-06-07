{% macro redshift__dropif(node) %}
    
    {% set ddl %}
        drop table if exists {{source(node.source_name, node.name)}} cascade
    {% endset %}
    
    {{return(ddl)}}

{% endmacro %}
