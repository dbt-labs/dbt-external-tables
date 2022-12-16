{% macro doris__refresh_external_table(source_node) %}    
    {% set refresh %}
        refresh catalog `{{source_node.source_name.split('.')[0]}}`
    {% endset %}
    {% do return([refresh]) %}
{% endmacro %}
