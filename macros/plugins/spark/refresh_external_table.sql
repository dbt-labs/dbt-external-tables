{% macro spark__refresh_external_table(source_node, relation) %}
    
    {% set refresh %}
        refresh table {{ relation }}
    {% endset %}
    
    {% do return([refresh]) %}

{% endmacro %}
