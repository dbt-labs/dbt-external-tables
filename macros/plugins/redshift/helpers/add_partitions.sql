
{#
  Generates a series of alter statements to add a batch of partitions to a table.
  Ideally it would require a single alter statement to add all partitions, but
  Amazon imposes a limit of 100 partitions per alter statement. Therefore we need
  to generate multiple altes when the number of partitions to add exceeds 100.

  Arguments:
    - source (string): The name of the table to generate the partitions for.
    - source_external_location (string): Base location of the external table. Paths
        in the 'partitions' argument are specified relative to this location.
    - partitions (list): A list of partitions to be added to the external table.
        Each partition is represented by a dictionary with the keys:
          - partition_by (list): A set of columns that the partition is affected by
              Each column is represented by a dictionary with the keys:
                - name: Name of the column
                - value: Value of the column
          - path (string): The path to be added as a partition for the particular
              combination of columns defined in the 'partition_by'
#}
{% macro redshift_alter_table_add_partitions(source_node, partitions) %}

  {{ log("Generating ADD PARTITION statement for partition set between " 
         ~ partitions[0]['path'] ~ " and " ~ (partitions|last)['path']) }}

  {% set ddl = [] %}
  
  {% if partitions|length > 0 %}
  
    {% set alter_table_add %}
        alter table {{source(source_node.source_name, source_node.name)}} add if not exists 
    {% endset %}
  
    {%- set alters -%}

      {{ alter_table_add }}

    {%- for partition in partitions -%}

      {%- if loop.index0 != 0 and loop.index0 % 100 == 0 -%}

        ; {{ alter_table_add }}

      {%- endif -%}

        partition ({%- for part in partition.partition_by -%}{{ part.name }}='{{ part.value }}'{{', ' if not loop.last}}{%- endfor -%})
        location '{{ source_node.external.location }}/{{ partition.path }}/'

    {% endfor -%}
    
    {%- endset -%}
    
    {% set ddl = ddl + alters.split(';') %}

  {% else %}

    {{ log("No partitions to be added") }}

  {% endif %}
  
  {% do return(ddl) %}

{% endmacro %}
