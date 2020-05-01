{%- for node in graph.nodes.values() -%}
    
    {%- if node.resource_type == 'source' and node.external.location != none -%}
    
        {{- dbt_utils.log_info('Staging external source ' ~ node.schema ~ '.' ~ node.identifier) -}}
        
        {%- set run_queue = dbt_external_tables.get_external_build_plan(node).split(';') -%}
        
        {%- for q in run_queue %}
            {{ q }}
            
            ----------
            
        {% endfor -%}
        
    {%- endif %}
    
{%- endfor -%}
