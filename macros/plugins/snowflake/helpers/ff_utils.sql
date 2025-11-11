{% macro parse_ff(file_format) %}
    {# Remove outer parenthesis and whitespaces within option assignment expressions #}
    {%- set ff_ddl = modules.re.sub('^\(|\)$|;$', '', file_format|trim) -%}
    {# Extract key=value pairs using regex. Handles values enclosed in parentheses or single quotes
         - Identifiers -> `\S+`
         - Quote string (escape-safe) -> `'(?:''|[^'])*'`
         - Parenthesis expressions -> `\(.*?\)`
    #}
    {%- set ff_opt_pairs = modules.re.findall("(\w+)\s*=\s*(\(.*?\)|'(?:''|[^'])*'|\S+)", ff_ddl, modules.re.S) -%}
    {# Consolidate case #}
    {%- set ff_opt_dict = dict() -%}
    {% for key, value in ff_opt_pairs %}
        {% do ff_opt_dict.update({key|lower: value}) %}
    {% endfor %}
    {{ return(ff_opt_dict) }}
{% endmacro %}


{% macro get_ff(file_format) %}
    {# 
        Returns a dictionary of file format options.
        If file_format is an inline definition, parses it directly.
        If file_format is a named format reference, fetches its DDL and parses that.
    #}
    {%- set parsed = parse_ff(file_format) -%}

    {# If parsing returned empty dict, it is a named format. Fetch DDL and re-parse #}
    {%- if not parsed -%}
        {% set ddl_query = "select get_ddl('FILE_FORMAT', '" ~ file_format ~ "') as ddl" %}
        {% set ddl_result = run_query(ddl_query) %}
        {% set ddl_string = ddl_result.columns[0].values()[0] %}
        {%- set parsed = parse_ff(ddl_string) -%}
    {%- endif -%}
    
    {{ return(parsed) }}
{% endmacro %}
