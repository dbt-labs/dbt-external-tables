{% macro redshift__exit_transaction() %}
    {{ return('begin; commit;') }}
{% endmacro %}
