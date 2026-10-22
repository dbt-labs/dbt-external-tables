{% macro snowflake__update_external_table_columns(source_node) %}

    {% if not execute %}
        {{ return(none) }}
    {% endif %}

    {% set relation = source(
        source_node.source_name,
        source_node.name
    ) %}

    {# The macro relies on YAML positions. If a column is inserted between existin col 1
        and existing col 2, the existing col 2 expression will still point to c2,
        even though the YAML now implies c3, therefore, append only new columns. #}

    {% set relation_columns = adapter.get_columns_in_relation(relation) %}
    {% set node_columns = source_node.columns.values() %}

    {% set existing_columns = relation_columns
        | map(attribute='name')
        | map('upper')
        | list
    %}

{% set is_csv = dbt_external_tables.is_csv(source_node.external.file_format) %}
{% set ignore_case = source_node.external.ignore_case or false %}

{% for column in node_columns %}

    {% set config_meta = column.get('config', {}).get('meta', {}) %}
    {% set legacy_meta = column.get('meta', {}) %}

    {% set alias = (
        column.get('alias')
        or config_meta.get('alias')
        or legacy_meta.get('alias')
    ) %}

    {% set name = alias or column.name %}
    {% set quoted = column.quote or false %}

    {% set actual_name = name if quoted else name | upper %}
    {% set column_name = adapter.quote(name) if quoted else name %}

    {% if actual_name not in existing_columns %}

        {% set expression = dbt_external_tables.snowflake__get_external_column_expression(
            column,
            loop.index,
            is_csv,
            ignore_case
        ) %}

        {% set sql %}
            ALTER TABLE {{ relation }}
            ADD COLUMN IF NOT EXISTS
                {{ column_name }} {{ column.data_type }}
            AS ({{ expression }}::{{ column.data_type }})
        {% endset %}

        {% do run_query(sql) %}

    {% endif %}

{% endfor %}

{% endmacro %}