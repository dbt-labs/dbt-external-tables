{% macro doris__create_external_table(source_node) %}

    {%- set external = source_node.external -%}
    {%- set options = external.options -%}
    

{# https://doris.apache.org/docs/dev/ecosystem/external-table/multi-catalog/ #}
{# This assumes you have already created an external catalog #}


    create catalog if not exists `{{source_node.source_name.split('.')[0]}}` PROPERTIES (
            'type' = '{{external.type}}' ,
        {% for column in options %}
            '{{column}}' = '{{options[column]}}'
            {{- ',' if not loop.last -}}
        {% endfor %}
    )
    
   
{% endmacro %}
