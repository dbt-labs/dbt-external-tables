{% macro snowflake__get_external_column_expression(
    column,
    position,
    is_csv,
    ignore_case=false
) %}

    {%- set column_quoted = adapter.quote(column.name)
        if column.quote else column.name -%}

    {%- if column.expression -%}
        {{ column.expression }}

    {%- elif 'expression' in column.meta -%}
        {{ column.meta.expression }}

    {%- else -%}
        {%- if is_csv -%}
            {%- set col_id = 'value:c' ~ position -%}
        {%- elif ignore_case -%}
            {%- set col_id =
                'GET_IGNORE_CASE($1, '
                ~ "'" ~ column_quoted ~ "'" ~ ')'
            -%}
        {%- else -%}
            {%- set col_id = 'value:' ~ column_quoted -%}
        {%- endif -%}

        (case
            when is_null_value({{ col_id }})
                or lower({{ col_id }}) = 'null'
            then null
            else {{ col_id }}
        end)

    {%- endif -%}

{% endmacro %}