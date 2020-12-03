{% macro year_month_day(name, value) %}
    {% set path = value.replace('-','/') %}
    {{return(path)}}
{% endmacro %}

{% macro key_value(name, value) %}
    {% set path = name ~ '=' ~ value %}
    {{return(path)}}
{% endmacro %}

{% macro value_only(name, value) %}
    {% set path = value %}
    {{return(path)}}
{% endmacro %}
