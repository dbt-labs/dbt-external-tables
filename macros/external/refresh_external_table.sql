{% macro refresh_external_table(source_node) %}
    {{ return(adapter.dispatch('refresh_external_table', 
        packages = dbt_external_tables._get_dbt_external_tables_namespaces()) 
        (source_node)) }}
{% endmacro %}

{% macro default__refresh_external_table(source_node) %}
    {{ exceptions.raise_compiler_error("External table creation is not implemented for the default adapter") }}
{% endmacro %}

{% macro redshift__refresh_external_table(source_node) %}

    {%- set starting = [
        {
            'partition_by': [],
            'path': ''
        }
    ] -%}

    {%- set ending = [] -%}
    {%- set finals = [] -%}
    
    {%- set partitions = source_node.external.get('partitions',[]) -%}

    {%- if partitions -%}{%- for partition in partitions -%}
    
        {%- if not loop.first -%}
            {%- set starting = ending -%}
            {%- set ending = [] -%}
        {%- endif -%}
        
        {%- for preexisting in starting -%}
            
            {%- if partition.vals.macro -%}
                {%- set vals = dbt_external_tables.render_from_context(partition.vals.macro, **partition.vals.args) -%}
            {%- elif partition.vals is string -%}
                {%- set vals = [partition.vals] -%}
            {%- else -%}
                {%- set vals = partition.vals -%}
            {%- endif -%}
        
            {%- for val in vals -%}
            
                {# For each preexisting guy, add a new one #}
            
                {%- set next_partition_by = [] -%}
                
                {%- for prexist_part in preexisting.partition_by -%}
                    {%- do next_partition_by.append(prexist_part) -%}
                {%- endfor -%}
                
                {%- do next_partition_by.append({'name': partition.name, 'value': val}) -%}

                {# Concatenate path #}

                {%- set concat_path = preexisting.path ~ '/' ~ dbt_external_tables.render_from_context(partition.path_macro, partition.name, val) -%}
                
                {%- do ending.append({'partition_by': next_partition_by, 'path': concat_path}) -%}
            
            {%- endfor -%}
            
        {%- endfor -%}
        
        {%- if loop.last -%}
            {%- for end in ending -%}
                {%- do finals.append(end) -%}
            {%- endfor -%}
        {%- endif -%}
        
    {%- endfor -%}
    
        {%- set ddl = dbt_external_tables.redshift_alter_table_add_partitions(source_node, finals) -%}
        {{ return(ddl) }}
    
    {% else %}
    
        {% do return([]) %}
    
    {% endif %}
    
{% endmacro %}

{% macro snowflake__refresh_external_table(source_node) %}

    {% set external = source_node.external %}
    {% set snowpipe = source_node.external.get('snowpipe', none) %}
    
    {% set auto_refresh = external.get('auto_refresh', false) %}
    {% set partitions = external.get('partitions', none) %}
    
    {% set manual_refresh = (partitions and not auto_refresh) %}
    
    {% if manual_refresh %}

        {% set ddl %}
        alter external table {{source(source_node.source_name, source_node.name)}} refresh
        {% endset %}
        
        {% do return([ddl]) %}
    
    {% else %}
    
        {% do return([]) %}
    
    {% endif %}
    
{% endmacro %}

{% macro bigquery__refresh_external_table(source_node) %}
    {{ exceptions.raise_compiler_error(
        "BigQuery does not support creating external tables in SQL/DDL. 
        Create it from the BQ console.") }}
{% endmacro %}

{% macro presto__refresh_external_table(source_node) %}
    {{ exceptions.raise_compiler_error(
        "Presto does not support creating external tables with 
        the Hive connector. Do so from Hive directly.") }}
{% endmacro %}
