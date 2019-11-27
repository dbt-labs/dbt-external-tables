{% macro is_csv(file_format) %}

{# From https://docs.snowflake.net/manuals/sql-reference/sql/create-external-table.html:

Important: The external table does not inherit the file format, if any, in the 
stage definition. You must explicitly specify any file format options for the 
external table using the FILE_FORMAT parameter.

Note: FORMAT_NAME and TYPE are mutually exclusive; to avoid unintended behavior, 
you should only specify one or the other when creating an external table.

#}

    {% set ff_ltrimmed = file_format|lower|replace(' ','') %}

    {% if 'type=' in ff_ltrimmed %}
    
        {% if 'type=csv' in ff_ltrimmed %}

            {{return(true)}}

        {% else %}

            {{return(false)}}
            
        {% endif %}
        
    {% else %}
    
        {% set ff_standardized = ff_ltrimmed
            | replace('(','') | replace(')','')
            | replace('format_name=','') %}
        {% set fqn = ff_standardized.split('.') %}
        
        {% if fqn | length == 3 %}
            {% set ff_database, ff_schema, ff_identifier = fqn[0], fqn[1], fqn[2] %}
        {% elif fqn | length == 2 %}
            {% set ff_database, ff_schema, ff_identifier = target.database, fqn[0], fqn[1] %}
        {% else %}
            {% set ff_database, ff_schema, ff_identifier = target.database, target.schema, fqn[0] %}
        {% endif %}
    
        {% call statement('get_file_format', fetch_result = True) %}
            show file formats in {{ff_database}}.{{ff_schema}}
        {% endcall %}
        
        {% set ffs = load_result('get_file_format').table %}
        
        {% for ff in ffs %}
            
            {% if ff['name']|lower == ff_identifier and ff['type']|lower == 'csv' %}
            
                {{return(true)}}
            
            {% endif %}
        
        {% endfor %}
        
        {{return(false)}}        
    
    {% endif %}

{% endmacro %}
