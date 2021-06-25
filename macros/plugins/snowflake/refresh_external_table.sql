{% macro snowflake__refresh_external_table(source_node) %}

    {% set external = source_node.external %}
    {% set snowpipe = source_node.external.get('snowpipe', none) %}
    
    {% set auto_refresh = external.get('auto_refresh', false) %}
    {% set partitions = external.get('partitions', none) %}
    
    {% set manual_refresh = (partitions and not auto_refresh) %}
    
    {% if manual_refresh %}

        {% set ddl %}
        BEGIN;
        alter external table {{source(source_node.source_name, source_node.name)}} refresh;
        COMMIT;
        {% endset %}
        
        {% do return([ddl]) %}
    
    {% else %}
    
        {% do return([]) %}
    
    {% endif %}
    
{% endmacro %}
