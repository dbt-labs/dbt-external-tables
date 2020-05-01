{% macro get_external_build_plan(source_node) %}
    {{ return(adapter_macro('dbt_external_tables.get_external_build_plan', source_node)) }}
{% endmacro %}

{% macro default__get_external_build_plan(source_node) %}
    {{ exceptions.raise_compiler_error("Staging external sources is not implemented for the default adapter") }}
{% endmacro %}

{% macro redshift__get_external_build_plan(source_node) %}

    {% set build_plan = [] %}

    {% set old_relation = adapter.get_relation(
        database = source_node.database,
        schema = source_node.schema,
        identifier = source_node.identifier
    ) %}
    
    {%- set partitions = source_node.external.get('partitions', none) -%}
    {% set create_or_replace = (partitions or old_relation is none or var('ext_full_refresh', false)) %}
    
    {% if create_or_replace %}

        {% set build_plan = [
            dbt_external_tables.dropif(source_node),
            dbt_external_tables.create_external_table(source_node),
            dbt_external_tables.refresh_external_table(source_node)
        ] %}
        
    {% else %}
    
        {{ dbt_utils.log_info('PASS') }}
        
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

{% macro stage_external_sources() %}
    
    {% for node in graph.nodes.values() %}
        
        {% if node.resource_type == 'source' and node.external.location != none %}
        
            {% do dbt_utils.log_info('Staging external source ' ~ node.schema ~ '.' ~ node.identifier) -%}
            
            {% set run_queue = get_external_build_plan(node) %}
            
            {% for q in run_queue %}
            
                {% call statement('runner', fetch_result = True, auto_begin = False) %}
                    {{ q }}
                {% endcall %}
                
                {% set status = load_result('runner')['status'] %}
                {% do dbt_utils.log_info('(' ~ loop.index ~ ') ' ~ status) %}
                
            {% endfor %}
            
        {% endif %}
        
    {% endfor %}
    
{% endmacro %}
