{% macro maxcompute__dropif(source_node) %}

    {% set ddl %}
        drop table if exists {{source(source_node.source_name, source_node.name)}}
    {% endset %}
    
    {{return(ddl)}}

{% endmacro %}
