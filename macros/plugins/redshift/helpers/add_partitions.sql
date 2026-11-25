
{% macro redshift_alter_table_add_partitions(source_node, relation, partitions) %}
  {% set ddl = [] %}
  {% if not partitions %}
    {% do log('No partitions to be added') %}
  {% endif %}

  {# Redshift Spectrum accepts at most 100 partitions per ALTER TABLE. #}
  {% for partition_batch in partitions | batch(100) %}
    {% do log('Generating ADD PARTITION statement for partition set between '
        ~ partition_batch[0]['path'] ~ ' and ' ~ (partition_batch | last)['path']) %}
    {% set sql %}
      alter table {{ relation }} add if not exists
      {% for partition in partition_batch %}
        partition ({%- for part in partition.partition_by -%}{{ part.name }}='{{ part.value }}'{{ ', ' if not loop.last }}{%- endfor -%})
        location '{{ source_node.external.location }}/{{ partition.path }}/'
      {% endfor %}
    {% endset %}
    {% do ddl.append(sql) %}
  {% endfor %}
  {% do return(ddl) %}
{% endmacro %}
