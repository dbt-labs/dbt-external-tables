{% macro databricks__recover_partitions(source_node) %}
    {# https://docs.databricks.com/sql/language-manual/sql-ref-syntax-ddl-alter-table.html #}

    {%- if source_node.external.partitions and source_node.external.using and source_node.external.using|lower != 'delta' -%}
        {% set ddl %}
            ALTER TABLE {{ source(source_node.source_name, source_node.name) }} RECOVER PARTITIONS
        {% endset %}
    {%- else -%}
        {% set ddl = none %}
    {%- endif -%}

    {{return(ddl)}}

{% endmacro %}