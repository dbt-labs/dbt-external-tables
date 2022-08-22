{% macro trino__refresh_external_table(source_node) %}
    {%- set partitions = source_node.external.partitions -%}
    {%- if partitions -%}
        {% set drop_partitions -%}
            call system.sync_partition_metadata('{{ source_node.source_name }}', '{{ source_node.name }}', 'DROP', false)
        {%- endset %}
        {% set add_partitions -%}
            call system.sync_partition_metadata('{{ source_node.source_name }}', '{{ source_node.name }}', 'ADD', false)
        {%- endset %}
        {{ return([drop_partitions, add_partitions]) }}
    {% endif %}
    {% do return([]) %}
{% endmacro %}
