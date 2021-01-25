{% macro snowflake__exit_transaction() %}
    {{ return('begin; commit;') }}
{% endmacro %}
