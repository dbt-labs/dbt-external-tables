{% macro stage_external_sources(select=none) %}

    {% set sources_to_stage = [] %}
    
    {% set source_nodes = graph.sources.values() if graph.sources else [] %}
    
    {% for node in source_nodes %}
        
            
            {% if select %}
            
                {% for src in select.split(' ') %}
                
                    {% if '.' in src %}
                        {% set src_s = src.split('.') %}
                        {% if src_s[0] == node.source_name and src_s[1] == node.name %}
                            {% do sources_to_stage.append(node) %}
                        {% endif %}
                    {% else %}
                        {% if src == node.source_name %}
                            {% do sources_to_stage.append(node) %}
                        {% endif %}
                    {% endif %}
                    
                {% endfor %}
                        
            {% else %}
            
                {% do sources_to_stage.append(node) %}
                
            {% endif %}
            
        
    {% endfor %}
            
    {% for node in sources_to_stage %}

        {% set loop_label = loop.index ~ ' of ' ~ loop.length %}

        {% do dbt_utils.log_info(loop_label ~ ' START external source ' ~ node.schema ~ '.' ~ node.identifier) -%}
        
        {% set run_queue = dbt_external_tables.get_external_build_plan(node) %}
        
        {% do dbt_utils.log_info(loop_label ~ ' SKIP') if run_queue == [] %}
        
        {% for q in run_queue %}
        
            {% set q_msg = q|trim %}
            {% set q_log = q_msg[:50] ~ '...  ' if q_msg|length > 50 else q_msg %}
        
            {% do dbt_utils.log_info(loop_label ~ ' (' ~ loop.index ~ ') ' ~ q_log) %}
            {% set exit_txn = dbt_external_tables.exit_transaction() %}
        
            {% call statement('runner', fetch_result = True, auto_begin = False) %}
                {{ exit_txn }} {{ q }}
            {% endcall %}
            
            {% set status = load_result('runner')['status'] %}
            {% do dbt_utils.log_info(loop_label ~ ' (' ~ loop.index ~ ') ' ~ status) %}
            
        {% endfor %}
        
    {% endfor %}
    
{% endmacro %}
