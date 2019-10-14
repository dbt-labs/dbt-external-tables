{% macro render_from_context(name) -%}
{% set original_name = name %}
  {% if '.' in name %}
    {% set package_name, name = name.split(".", 1) %}
  {% else %}
    {% set package_name = none %}
  {% endif %}

  {% if package_name is none %}
    {% set package_context = context %}
  {% elif package_name in context %}
    {% set package_context = context[package_name] %}
  {% else %}
    {% set error_msg %}
        In adapter_macro: could not find package '{{package_name}}', called with '{{original_name}}'
    {% endset %}
    {{ exceptions.raise_compiler_error(error_msg | trim) }}
  {% endif %}
  
    {{ return(package_context[name](*varargs, **kwargs)) }}

{%- endmacro %}

{% macro dropif(node) %}

    {% set fqn = [node.database, node.schema, node.identifier]|join('.') %}
    {# snowflake returns 'externaltable' as relation type,
    which should be 'external table' for dropping,
    this is part of BaseRelation def #}
    {% set reltype = 'external table' if target.type == 'snowflake' else 'table' %}
    
    {% set ddl %}
        drop {{reltype}} if exists {{fqn}} cascade
    {% endset %}
    
    {{return(ddl)}}

{% endmacro %}
