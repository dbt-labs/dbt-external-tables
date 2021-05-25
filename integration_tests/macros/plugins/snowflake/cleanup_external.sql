{% macro snowflake__cleanup_external() %}

    {% set unset_autocommit %}
        alter user {{ target.user }} unset autocommit;
    {% endset %}

    {% do log('Unsetting autocommit parameter for user ' ~ target.user, info = true) %}
    {% do run_query(unset_autocommit) %}
    
{% endmacro %}
