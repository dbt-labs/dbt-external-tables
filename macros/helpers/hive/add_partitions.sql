{%- macro generate_hive_partitions(partitions) -%}

    {%- set starting = [
        {
            'partition_by': [],
            'path': ''
        }
    ] -%}

    {%- set ending = [] -%}
    {%- set finals = [] -%}

    {%- for partition in partitions -%}

        {%- if not loop.first -%}
            {%- set starting = ending -%}
            {%- set ending = [] -%}
        {%- endif -%}
        
        {%- for preexisting in starting -%}
            
            {%- if partition.vals.macro -%}
                {%- set vals = render_from_context(partition.vals.macro, **partition.vals.args) -%}
            {%- elif partition.vals is string -%}
                {%- set vals = [partition.vals] -%}
            {%- else -%}
                {%- set vals = partition.vals -%}
            {%- endif -%}
        
            {%- for val in vals -%}
            
                {# For each preexisting guy, add a new one #}
            
                {%- set next_partition_by = [] -%}
                
                {%- for prexist_part in preexisting.partition_by -%}
                    {%- do next_partition_by.append(prexist_part) -%}
                {%- endfor -%}
                
                {%- do next_partition_by.append({'name': partition.name, 'value': val}) -%}

                {# Concatenate path #}

                {%- set concat_path = preexisting.path ~ '/' ~ render_from_context(partition.path_macro, partition.name, val) -%}
                
                {%- do ending.append({'partition_by': next_partition_by, 'path': concat_path}) -%}
            
            {%- endfor -%}
            
        {%- endfor -%}
        
        {%- if loop.last -%}
            {%- for end in ending -%}
                {%- do finals.append(end) -%}
            {%- endfor -%}
        {%- endif -%}
        
    {%- endfor -%}
    
    {%- do return(finals) -%}

{% endmacro %}

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

{% macro redshift__alter_table_add_partitions(source, source_external_location, partitions) %}

  {{ log("Generating ADD PARTITION statement for partition set \n" ~ partitions) }}

  {% if partitions|length > 0 %}

      alter table {{ source }} add

    {% for partition in partitions %}

      {% if loop.index0 != 0 and loop.index0 % 100 == 0 %}

        ; -- close alter statement and open a new one
        alter table {{ source }} add

      {% endif %}

        partition ({%- for part in partition.partition_by -%}{{ part.name }}='{{ part.value }}'{{',' if not loop.last}}{%- endfor -%})
        location '{{ source_external_location }}{{ partition.path }}/'

    {% endfor %}

  {% else %}

    {{ log("No partitions to be added") }}

  {% endif %}

{% endmacro %}
