test-redshift:
	dbt deps --target redshift
	dbt run-operation prep_external --target redshift
	dbt run-operation stage_external_sources --var 'ext_full_refresh: true' --target redshift
	dbt run-operation stage_external_sources --target redshift
	
test-snowflake:
	dbt deps --target snowflake
	dbt run-operation prep_external --target snowflake
	dbt run-operation stage_external_sources --var 'ext_full_refresh: true' --target snowflake
	dbt run-operation stage_external_sources --target snowflake
