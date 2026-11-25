{% macro snowflake__refresh_external_table(source_node, relation) %}

    {% set external = source_node.external %}
    
    {% set auto_refresh = external.get('auto_refresh', false) %}
    
    {% set manual_refresh = not auto_refresh %}
    
    {% if manual_refresh %}

        {% set ddl %}
        begin;
        alter external table {{ relation }} refresh;
        commit;
        {% endset %}
        
        {% do return([ddl]) %}
    
    {% else %}
    
        {% do return([]) %}
    
    {% endif %}
    
{% endmacro %}
