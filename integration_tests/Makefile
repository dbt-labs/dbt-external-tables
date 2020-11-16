test-redshift:
	dbt deps --target redshift
	dbt run-operation prep_external --target redshift
	dbt run-operation stage_external_sources --var 'ext_full_refresh: true' --target redshift
	dbt run-operation stage_external_sources --target redshift
	dbt seed --target redshift
	dbt test --target redshift
	
test-snowflake:
	dbt deps --target snowflake
	dbt run-operation prep_external --target snowflake
	dbt run-operation stage_external_sources --var 'ext_full_refresh: true' --target snowflake
	dbt run-operation stage_external_sources --target snowflake
	dbt seed --target snowflake
	dbt test --target snowflake
