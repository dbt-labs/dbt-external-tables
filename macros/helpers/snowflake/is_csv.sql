{% macro is_csv(file_format) %}

{# From https://docs.snowflake.net/manuals/sql-reference/sql/create-external-table.html:

Important: The external table does not inherit the file format, if any, in the 
stage definition. You must explicitly specify any file format options for the 
external table using the FILE_FORMAT parameter.

Note: FORMAT_NAME and TYPE are mutually exclusive; to avoid unintended behavior, 
you should only specify one or the other when creating an external table.

#}

    {% if 'format_name' in file_format %}
    
        {% set file_format_identifier = file_format.split('.')|last %}
    
        {% call statement('get_file_format', fetch_results = True) %}
            show file formats like '{{file_format.split()}}'
        {% endcall %}
        
        {% set ff_type = load_result('get_file_format').table.columns['TYPE'] %}
        
        {% if ff_type == 'csv' %}
        
            {{return(true)}}
            
        {% else %}
        
            {{return(false)}}
            
        {% endif %}
            
    {% else %}

        {% set ff_standardized = file_format|lowercase|replace(' ','') %}
        
        {% if 'type=csv' in ff_standardized %}

            {{return(true)}}

        {% else %}
    
            {{return(false)}}
            
        {% endif %}
    
    {% endif %}

{% endmacro %}
