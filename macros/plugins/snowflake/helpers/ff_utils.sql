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
        Returns a tuple of (file format name, file format options).
        If file_format is a named format reference, fetches its DDL and parses that.
    #}
    {%- set parsed = parse_ff(file_format) -%}
    {%- if parsed -%}
        {{ return((none, parsed)) }}
    {%- endif -%}

    {# If parsing returned an empty dict, it is a named format. Fetch DDL and re-parse. #}
    {%- set ff_name = file_format|trim -%}
    {% set ddl_query = "select get_ddl('FILE_FORMAT', '" ~ ff_name ~ "') as ddl" %}
    {% set ddl_result = run_query(ddl_query) %}
    {% set ddl_string = ddl_result.columns[0].values()[0] %}

    {{ return((ff_name, parse_ff(ddl_string))) }}
{% endmacro %}
