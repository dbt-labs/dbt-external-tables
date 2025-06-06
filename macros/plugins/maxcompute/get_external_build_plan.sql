{% macro maxcompute__get_external_build_plan(source_node) %}

    {% set build_plan = [] %}
    
    {% set create_or_replace = (var('ext_full_refresh', false)) %}
    
    {% if create_or_replace %}

        {% set build_plan = [
                dbt_external_tables.create_external_schema(source_node),
                dbt_external_tables.dropif(source_node),
                dbt_external_tables.create_external_table(source_node)
            ] + dbt_external_tables.refresh_external_table(source_node) 
        %}
        
    {% else %}
    
        {% set build_plan = dbt_external_tables.refresh_external_table(source_node) %}
        
    {% endif %}
    
    {% do return(build_plan) %}

{% endmacro %}
