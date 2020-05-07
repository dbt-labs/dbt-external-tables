{% macro redshift_is_ext_tbl(node) %}

    {% set existing_relation = load_relation(node) %}
    
    {#  external tables don't appear in information_schema.tables,
        so dbt doesn't cache them #}
    {% if existing_relation is none %}

        {% set find_ext_tbl %}
        
            select count(*) from svv_external_tables
            where schemaname = '{{node.schema}}'
            and tablename = '{{node.identifier}}'
        
        {% endset %}
        
        {% if execute %}
            {% set result = run_query(find_ext_tbl)[0][0] %}
        {% else %}
            {% set result = 0 %}
        {% endif %}

        {% set is_ext_tbl = (result > 0) %}
        {% do return(is_ext_tbl) %}
        
    {% else %}
    
        {% do return(false) %}
        
    {% endif %}

{% endmacro %}
