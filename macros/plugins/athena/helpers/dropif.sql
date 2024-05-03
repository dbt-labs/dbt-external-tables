{% macro athena__dropif(node) %}
    {% set ddl %}
        drop table if exists {{source(node.source_name, node.name).render_hive()}}
    {% endset %}
    {{return(ddl)}}
{% endmacro %}