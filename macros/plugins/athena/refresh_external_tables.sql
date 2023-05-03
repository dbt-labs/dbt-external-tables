{% macro athena__refresh_external_table(source_node) %}
    {# https://docs.aws.amazon.com/athena/latest/ug/partitions.html #}
    {%- set partitions = source_node.external.partitions -%}
    {%- set hive_compatible_partitions = source_node.external.get('hive_compatible_partitions', false) -%}
    {%- if partitions -%}
        {%- if hive_compatible_partitions -%}
            {% set ddl -%}
                msck repair table {{source(source_node.source_name, source_node.name).render_hive()}}
            {%- endset %}
            {{ return([ddl]) }}
        {% else %}
            {# https://docs.aws.amazon.com/athena/latest/ug/alter-table-add-partition.html #}
            {%- set part_len = partitions|length -%}
            {%- set get_partitions_sql -%}
            select * from
            {%- for partition in partitions %} (
                {%- set part_num = loop.index -%}
                {%- if partition.vals.macro -%}
                    {%- set vals = dbt_external_tables.render_from_context(partition.vals.macro, **partition.vals.args) -%}
                {%- elif partition.vals is string -%}
                    {%- set vals = [partition.vals] -%}
                {%- else -%}
                    {%- set vals = partition.vals -%}
                {%- endif -%}
                {%- for val in vals %}
                    select
                        '"{{ partition.name }}"' as name_{{ part_num }},
                        '"{{ val }}"' as val_{{ part_num }},
                        '"{{ dbt_external_tables.render_from_context(partition.path_macro, partition.name, val) }}"' as path_{{ part_num }}
                    {{ 'union all' if not loop.last else ') ' }}
                {%- endfor -%}
                {{ 'cross join' if not loop.last }}
            {%- endfor -%}
            {%- endset -%}
            {%- set finals = [] -%}
            {%- if execute -%}
                {%- set results = run_query(get_partitions_sql) -%}
                {%- for row in results -%}
                    {%- set partition_parts = [] -%}
                    {%- set path_parts = [] -%}
                    {%- for i in range(0, part_len) -%}
                        {%- do partition_parts.append({
                            'name': row[i * 3][1:-1],
                            'value': row[i * 3 + 1][1:-1]
                        }) -%}
                        {%- do path_parts.append(row[i * 3 + 2][1:-1]) -%}
                    {%- endfor -%}
                    {%- set construct = {
                        'partition_by': partition_parts,
                        'path': path_parts | join('/')
                    }  -%}
                    {% do finals.append(construct) %}
                {%- endfor -%}
            {%- endif -%}
            {%- set ddl = dbt_external_tables.redshift_alter_table_add_partitions(source_node, finals, is_athena=True) -%}
            {{ return(ddl) }}
            {% do return([]) %}
        {% endif %}
    {% endif %}
    {% do return([]) %}
{% endmacro %}