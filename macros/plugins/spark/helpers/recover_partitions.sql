{% macro spark__recover_partitions(source_node, relation) %}
    {# https://docs.databricks.com/sql/language-manual/sql-ref-syntax-ddl-alter-table.html #}

    {%- if source_node.external.partitions and source_node.external.using and source_node.external.using|lower != 'delta' -%}
        {% set ddl %}
            ALTER TABLE {{ relation }} RECOVER PARTITIONS
        {% endset %}
    {%- else -%}
        {% set ddl = none %}
    {%- endif -%}

    {{return(ddl)}}

{% endmacro %}

{% macro recover_partitions(source_node, relation) %}
    {{ return(adapter.dispatch('recover_partitions', 'dbt_external_tables')(source_node, relation)) }}
{% endmacro %}

{% macro default__recover_partitions(source_node, relation) %}
    /*{# 
        We're dispatching this macro so that users can override it if required on other adapters
        but this will work for spark/databricks. 
    #}*/

    {{ exceptions.raise_not_implemented('recover_partitions macro not implemented for adapter ' + adapter.type()) }}
{% endmacro %}
