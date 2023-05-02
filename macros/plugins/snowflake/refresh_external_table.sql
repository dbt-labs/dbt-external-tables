{% macro snowflake__refresh_external_table(source_node) %}

    {% set external = source_node.external %}
    {% if not external.get('auto_refresh', false) %}

        {% set ddl %}
        begin;
        alter external table {{source(source_node.source_name, source_node.name)}} refresh;
        commit;
        {% endset %}
        
        {% do return([ddl]) %}
    
    {% else %}
    
        {% do return([]) %}
    
    {% endif %}
    
{% endmacro %}
