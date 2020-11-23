{% macro get_external_build_plan(source_node) %}
    {{ return(adapter.dispatch('get_external_build_plan',
        packages = dbt_external_tables._get_dbt_external_tables_namespaces())
        (source_node)) }}
{% endmacro %}

{% macro default__get_external_build_plan(source_node) %}
    {{ exceptions.raise_compiler_error("Staging external sources is not implemented for the default adapter") }}
{% endmacro %}

{% macro redshift__get_external_build_plan(source_node) %}

    {% set build_plan = [] %}
    
    {%- set partitions = source_node.external.get('partitions', none) -%}
    {% set create_or_replace = (var('ext_full_refresh', false) or not redshift_is_ext_tbl(source_node)) %}
    
    {% if create_or_replace %}

        {% set build_plan = [
                dbt_external_tables.dropif(source_node),
                dbt_external_tables.create_external_table(source_node)
            ] + dbt_external_tables.refresh_external_table(source_node) 
        %}
        
    {% else %}
    
        {% set build_plan = dbt_external_tables.refresh_external_table(source_node) %}
        
    {% endif %}
    
    {% do return(build_plan) %}

{% endmacro %}

{% macro snowflake__get_external_build_plan(source_node) %}

    {% set build_plan = [] %}
    
    {% set old_relation = adapter.get_relation(
        database = source_node.database,
        schema = source_node.schema,
        identifier = source_node.identifier
    ) %}
    
    {% set create_or_replace = (old_relation is none or var('ext_full_refresh', false)) %}

    {% if source_node.external.get('snowpipe', none) is not none %}
    
        {% if create_or_replace %}
            {% set build_plan = build_plan + [
                dbt_external_tables.snowflake_create_empty_table(source_node),
                dbt_external_tables.snowflake_get_copy_sql(source_node),
                dbt_external_tables.snowflake_create_snowpipe(source_node)
            ] %}
        {% else %}
            {% set build_plan = build_plan + dbt_external_tables.snowflake_refresh_snowpipe(source_node) %}
        {% endif %}
            
    {% else %}
    
        {% if create_or_replace %}
            {% set build_plan = build_plan + [dbt_external_tables.create_external_table(source_node)] %}
        {% else %}
            {% set build_plan = build_plan + dbt_external_tables.refresh_external_table(source_node) %}
        {% endif %}
        
    {% endif %}

    {% do return(build_plan) %}

{% endmacro %}

{% macro stage_external_sources(select=none) %}

    {% set sources_to_stage = [] %}
    
    {% set source_nodes = graph.sources.values() if graph.sources else [] %}
    
    {% for node in source_nodes %}
        
        {% if node.external.location %}
            
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
