{% macro redshift_is_ext_tbl(relation) %}
    {% if not execute %}
        {% do return(false) %}
    {% endif %}

    {# The relation cache does not reliably distinguish Spectrum external tables. #}
    {% set find_ext_tbl %}
        select count(*) from svv_external_tables
        where redshift_database_name = '{{ dbt.escape_single_quotes(relation.database) }}'
          and schemaname = '{{ dbt.escape_single_quotes(relation.schema) }}'
          and tablename = '{{ dbt.escape_single_quotes(relation.identifier) }}'
    {% endset %}
    {% do return(run_query(find_ext_tbl)[0][0] > 0) %}
{% endmacro %}
