{% macro spark__refresh_external_table(source_node) %}
    
    {% set refresh %}
        refresh table {{ this }}
    {% endset %}
    
    {% do return([refresh]) %}

{% endmacro %}
