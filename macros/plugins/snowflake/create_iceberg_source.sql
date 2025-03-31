{% macro snowflake_create_iceberg_source(source_node) %}

    {% set relation = api.Relation.create(
        database = source_node.database,
        schema = source_node.schema,
        identifier = source_node.identifier
    ) %}

    {% set required_configs = ['external_volume', 'catalog', 'catalog_table_name', 'catalog_namespace'] %}
    {% set optional_configs = ['replace_invalid_characters', 'auto_refresh', 'comment'] %}

    {% set ddl %}
        create or replace iceberg table {{ relation }}
        {% for config in required_configs %}
            {{ config }} = '{{ source_node.external.get(config) }}'
        {%- endfor -%}

        {% for config in optional_configs %}
            {% if config in source_node.external -%}

                {%- if source_node.external.get(config) is boolean -%}
                    {{ config }} = {{ source_node.external.get(config) }}

                {%- else -%}
                    {{ config }} = '{{ source_node.external.get(config) }}'
                {%- endif -%}

            {%- endif -%}
        {%- endfor -%}
        
        ;
    {% endset %}

    {{ ddl }}

{% endmacro %}