{% macro stage_external_sources() %}
    
    {% for node in graph.nodes.values() %}
        
        {% if node.resource_type == 'source' and node.external != none %}
            
            {%- set run_queue = [
                dropif(node),
                create_external_table(node)
            ] -%}
            
            {%- if node.external.partitions -%}
                {%- set run_queue = run_queue + refresh_external_table(node).split(';') -%}
            {%- endif -%}
            
            {% for q in (run_queue) %}
            
                {% call statement('runner', fetch_result = True, auto_begin = False) %}
                    {{ q }}
                {% endcall %}
                
                {% set status = load_result('runner')['status'] %}
                {% do log(loop.index ~ '. ' ~ status, info = true) %}
                
            {% endfor %}
            
        {% endif %}
        
    {% endfor %}
    
{% endmacro %}
