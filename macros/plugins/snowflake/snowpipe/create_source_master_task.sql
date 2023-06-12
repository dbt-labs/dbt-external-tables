{% macro snowflake_create_source_master_task(source_node) %}

    CREATE TASK IF NOT EXISTS {{ source_node.source_name }}_TSK
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    SCHEDULE = '{{ source_node.meta.task_schedule }}'
    {% if target.name == 'prod' %}
    SUSPEND_TASK_AFTER_NUM_FAILURES = 1
    ERROR_INTEGRATION = CR_NI_AWS_ERROR
    {% endif %}
    COMMENT = "Master task for {{ source_node.source_name }}. Is starting point for all tasks in schema unless overwritten explicitly."
    AS
    SELECT NULL;

    CREATE OR REPLACE TASK {{ source(source_node.source_name, source_node.name) }}_TSK
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    {% if target.name == 'prod' %}
    SUSPEND_TASK_AFTER_NUM_FAILURES = 1
    ERROR_INTEGRATION = CR_NI_AWS_ERROR
    {% endif %}
    COMMENT = "Sub Master task for {{ source(source_node.source_name, source_node.name) }}. Is starting point for all tasks dependent on this source table unless overwritten explicitly."
    AFTER {{ source_node.source_name }}_TSK
    AS
    SELECT NULL;

    {% if target.name == 'prod' %}
    -- RESUME tasks if production
    ALTER TASK {{ source_node.source_name }}_TSK RESUME;
    ALTER TASK {{ source(source_node.source_name, source_node.name) }}_TSK RESUME;
    {% endif -%}

{% endmacro %}
