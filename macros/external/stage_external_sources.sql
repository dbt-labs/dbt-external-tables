{% macro get_external_build_plan(source_node) %}
    {{ adapter_macro('dbt_external_tables.get_external_build_plan', source_node) }}
{% endmacro %}

{% macro default__get_external_build_plan(source_node) %}
    {{ exceptions.raise_compiler_error("Staging external sources is not implemented for the default adapter") }}
{% endmacro %}

{% macro redshift__get_external_build_plan(source_node) %}

    {% set build_plan = 
        dropif(source_node) + ';' + 
        create_external_table(source_node) + ';' + 
        refresh_external_table(source_node)
    %}
    
    {% do return(build_plan) %}

{% endmacro %}

{% macro snowflake__get_external_build_plan(source_node) %}

    {% set build_plan %}

    {% if source_node.external.snowpipe == true %}
    
        {% set old_relation = adapter.get_relation(
            database = source_node.database, 
            schema = source_node.schema, 
            identifier = source_node.identifier
        ) %}
        {% if old_relation is none %}
            {{ dbt_external_tables.snowflake_create_empty_table(source_node) }};
        {% endif %}
        
            {{ dbt_external_tables.snowflake_create_snowpipe(source_node) }}
            
    {% else %}

            {{ create_external_table(source_node) }}
        
    {% endif %}
    {% endset %}

    {% do return(build_plan) %}

{% endmacro %}

{% macro stage_external_sources() %}
    
    {% for node in graph.nodes.values() %}
        
        {% if node.resource_type == 'source' and node.external.location != none %}
        
            {% set ts = modules.datetime.datetime.now().strftime('%H:%M:%S') %}
            {%- do log(ts ~ ' + Staging external source ' ~ node.schema ~ '.' ~ node.identifier, info = true) -%}
            
            {% set run_queue = get_external_build_plan(node).split(';') %}
            
            {% for q in run_queue %}
            
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
