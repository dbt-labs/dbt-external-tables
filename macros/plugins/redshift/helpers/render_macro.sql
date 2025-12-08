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
        Could not find package '{{package_name}}', called by macro '{{original_name}}'
    {% endset %}
    {{ exceptions.raise_compiler_error(error_msg | trim) }}
  {% endif %}
  
  {# hack to workaround dbt-fusion/issues/787 when using Fusion #}
  {% if name in package_context %}
    {{ return(package_context[name](*varargs, **kwargs)) }}
  {% elif name == 'convert_datetime' %}
    {{ return(convert_datetime(*varargs, **kwargs)) }}
  {% elif name == 'dates_in_range' %}
    {{ return(dates_in_range(*varargs, **kwargs)) }}
  {% elif name == 'partition_range' %}
    {{ return(partition_range(*varargs, **kwargs)) }}
  {% elif name == 'py_current_timestring' %}
    {{ return(py_current_timestring(*varargs, **kwargs)) }}
  {% elif name == 'statement' %}
    {{ return(statement(*varargs, **kwargs)) }}
  {% elif name == 'noop_statement' %}
    {{ return(noop_statement(*varargs, **kwargs)) }}
  {% elif name == 'run_query' %}
    {{ return(run_query(*varargs, **kwargs)) }}
  {% else %}
    {% set error_msg %}
        Could not find macro '{{ original_name }}' within the context for '{{ package_name }}' so 'package_context["{{ name }}"]' will raise an error. Try calling '{{original_name}}(...)' directly instead.
    {% endset %}
    {{ exceptions.raise_compiler_error(error_msg | trim) }}
  {% endif %}


{%- endmacro %}
