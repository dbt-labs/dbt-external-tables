{% macro spark__recover_partitions(source_node) %}
    {# https://docs.databricks.com/sql/language-manual/sql-ref-syntax-ddl-alter-table.html #}

    {% set ddl %}
    {%- if (source_node.external.partitions or source_node.external.recover_partitions) and source_node.external.using and source_node.external.using|lower != 'delta' -%}
        ALTER TABLE {{ source(source_node.source_name, source_node.name) }} RECOVER PARTITIONS
    {%- endif -%}
    {% endset %}

    {{return(ddl)}}

{% endmacro %}
