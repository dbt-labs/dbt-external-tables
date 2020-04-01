{% macro stage_external_sources() %}
    
    {% for node in graph.nodes.values() %}
        
        {% if node.resource_type == 'source' and node.external.location != none %}
        
            {% set ts = modules.datetime.datetime.now().strftime('%H:%M:%S') %}
            {%- do log(ts ~ ' + Staging external table ' ~ node.schema ~ '.' ~ node.identifier, info = true) -%}
            
            {%- set run_queue = [] -%}
            
            {%- if target.type != 'snowflake' -%}
                {# Snowflake supports "create or replace" #}
                {%- do run_queue.append(dropif(node))  -%}
            {%- endif -%}
            
            {%- do run_queue.append(create_external_table(node)) -%}
            
            {%- if node.external.partitions and target.type == 'redshift' -%}
                {%- set run_queue = run_queue + refresh_external_table(node).split(';') -%}
            {%- endif -%}
            
            {% for q in (run_queue) %}
            
                {% call statement('runner', fetch_result = True, auto_begin = False) %}
                    {{ q }}
                {% endcall %}
                
                {% set ts = modules.datetime.datetime.now().strftime('%H:%M:%S') %}
                {% set status = load_result('runner')['status'] %}
                {% set msg = ts ~ ' + (' ~ loop.index ~ ') ' ~ status %}
                {% do log(msg, info = true) %}
                
            {% endfor %}
            
        {% endif %}
        
    {% endfor %}
    
{% endmacro %}
