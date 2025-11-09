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
    {%- set ff_opt_dict = parse_ff(file_format) -%}
    {%- set ff_name = ff_opt_dict.get('format_name', none) if ff_opt_dict else file_format -%}

    {%- if ff_name -%}
        {% set get_ddl_query = "select get_ddl('FILE_FORMAT', '" ~ ff_name ~ "') as ddl" %}
        {# {% do log('get_ddl_query: ' ~ get_ddl_query, info=True) %} #}
        {% set ddl_result = run_query(get_ddl_query) %}
        {% set ddl_str = ddl_result.columns[0].values()[0] %}
        {% set ff_opt_dict = parse_ff(ddl_str) %}
    {% endif %}
    {{ return(ff_opt_dict) }}
{% endmacro %}
