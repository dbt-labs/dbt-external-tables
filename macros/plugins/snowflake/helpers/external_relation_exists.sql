{% macro snowflake_external_relation_exists() %}
    {% set adapter_quoting = adapter.Relation.create().quote_policy %}
    {% if this.quote_policy == adapter_quoting %}
        {% do return(load_relation(this) is not none) %}
    {% endif %}

    {# get_relation uses adapter quoting, so match the stored name in the resolved source schema. #}
    {% set identifier = this.identifier %}
    {% set ignore_case = not this.quote_policy.identifier %}
    {% if this.quote_policy.identifier and identifier != identifier | upper %}
        {% set parameters = run_query("show parameters like 'QUOTED_IDENTIFIERS_IGNORE_CASE' in session") %}
        {% set ignore_case = parameters.rows[0]['value'] | lower == 'true' %}
    {% endif %}
    {% set identifier = identifier | upper if ignore_case else identifier %}
    {% set relations = adapter.list_relations_without_caching(this.without_identifier()) %}
    {% do return(relations | selectattr('identifier', 'equalto', identifier) | first is defined) %}
{% endmacro %}
