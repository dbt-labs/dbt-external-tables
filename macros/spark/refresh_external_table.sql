{% macro spark__refresh_external_table(source_node) %}
    
    {% set refresh %}
        refresh table {{source(source_node.source_name, source_node.name)}}
    {% endset %}
    
    {% do return([refresh]) %}

{% endmacro %}
