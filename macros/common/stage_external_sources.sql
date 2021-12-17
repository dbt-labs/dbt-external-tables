{% macro stage_external_sources(select=none) %}

    {% set sources_to_stage = [] %}
    
    {% set source_nodes = graph.sources.values() if graph.sources else [] %}
    
    {% for node in source_nodes %}
        {% if node.external %}
            
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
        {% endif %}
        
    {% endfor %}
    
    {% if sources_to_stage|length == 0 %}
        {% do dbt_utils.log_info('No external sources selected') %}
    {% endif %}
            
    {% for node in sources_to_stage %}

        {% set loop_label = loop.index ~ ' of ' ~ loop.length %}

        {% do dbt_utils.log_info(loop_label ~ ' START external source ' ~ node.schema ~ '.' ~ node.identifier) -%}
        
        {% set run_queue = dbt_external_tables.get_external_build_plan(node) %}
        
        {% do dbt_utils.log_info(loop_label ~ ' SKIP') if run_queue == [] %}
        {% set width = 80 %} {# hard code this for now, use PRINTER_WIDTH flag in v1.0+ #}
        
        {% for q in run_queue %}
        
            {% set q_msg = q|replace('\n','')|replace('begin;','')|trim %}
            {% set q_log = q_msg[:width] ~ '...  ' if q_msg|length > width else q_msg %}
        
            {% do dbt_utils.log_info(loop_label ~ ' (' ~ loop.index ~ ') ' ~ q_log) %}
            {% set exit_txn = dbt_external_tables.exit_transaction() %}
        
            {% call statement('runner', fetch_result = True, auto_begin = False) %}
                {{ exit_txn }} {{ q }}
            {% endcall %}
            
            {% set runner = load_result('runner') %}
            {% set log_msg = runner['response'] if 'response' in runner.keys() else runner['status'] %}
            {% do dbt_utils.log_info(loop_label ~ ' (' ~ loop.index ~ ') ' ~ log_msg) %}
            
        {% endfor %}
        
    {% endfor %}
    
{% endmacro %}
