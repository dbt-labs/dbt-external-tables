test-redshift:
	dbt deps --target redshift
	dbt seed --full-refresh --target redshift
	dbt run-operation prep_external --target redshift
	dbt run-operation stage_external_sources --var 'ext_full_refresh: true' --target redshift
	dbt run-operation stage_external_sources --target redshift
	dbt test --target redshift
	
test-snowflake:
	dbt deps --target snowflake
	dbt seed --full-refresh --target snowflake
	dbt run-operation prep_external --target snowflake
	dbt run-operation stage_external_sources --var 'ext_full_refresh: true' --target snowflake
	dbt run-operation stage_external_sources --target snowflake
	dbt test --target snowflake

test-databricks:
	dbt deps --target databricks
	dbt seed --full-refresh --target databricks
	dbt run-operation prep_external --target databricks
	dbt run-operation stage_external_sources --var 'ext_full_refresh: true' --target databricks
	dbt run-operation stage_external_sources --target databricks
	dbt test --target databricks
