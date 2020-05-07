{%- for node in graph.nodes.values() -%}
    
    {%- if node.resource_type == 'source' and node.external.location != none -%}
    
        {{ 'Staging external source ' ~ node.schema ~ '.' ~ node.identifier }}
        
        {%- set run_queue = dbt_external_tables.get_external_build_plan(node) -%}
        
        {%- for q in run_queue %}
            {{ q }}
            
            ----------
            
        {% endfor -%}
        
    {%- endif %}
    
{%- endfor -%}
