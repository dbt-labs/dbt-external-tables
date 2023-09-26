{% macro athena__create_external_table(source_node) %}

    {%- set columns = source_node.columns.values() -%}
    {%- set external = source_node.external -%}
    
    create external table {{source(source_node.source_name, source_node.name).render_hive()}} (
    {% for column in columns %}
        {{column.name}} {{column.data_type}}
        {{- ',' if not loop.last -}}
    {% endfor %}
    )
    {% if external.comment -%} comment '{{external.comment}}' {%- endif %}
    {% if external.partitions -%}
        partitioned by (
        {% for partition in external.partitions %}
            {{partition.name}} {{partition.data_type}}
            {%- if partition.comment %} comment '{{partition.comment}}' {%- endif -%}
            {{- ', ' if not loop.last -}}
        {% endfor %}
        )
    {%- endif %}
    {% if external.clusters and external.num_buckets -%}
        clustered by (
        {%- for column in external.clusters -%}
            {{column}}{{', ' if not loop.last}}
        {%- endfor -%}
        ) into num_buckets {{external.num_buckets}}
    {%- endif %}
    {% if external.row_format -%} row format {{external.row_format}} {%- endif %}
    {% if external.file_format -%} stored as {{external.file_format}} {%- endif %}
    {% if external.serde_properties -%} with serdeproperties {{external.serde_properties}} {%- endif %}
    {% if external.location -%} location '{{external.location}}' {%- endif %}
    {% if external.table_properties -%} tblproperties {{external.table_properties}} {%- endif %}
    ;
{% endmacro %}