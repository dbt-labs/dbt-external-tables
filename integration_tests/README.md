## Integration tests

The files in `public_data` are available in two public storage buckets:
- `s3://dbt-external-tables-testing`
- `gs://dbt-external-tables-testing/`

These integration tests confirm that, when staged as external tables, using different databases / file formats / partitioning schemes, the final combined output is equivalent to `seeds/people.csv`.

### Snowflake per-node staging preview

Use a dbt Fusion build that supports `run-operation --per-node` and an isolated Snowflake test schema. Run from `integration_tests` after configuring the `snowflake` profile target.

```bash
dbt deps --target snowflake
dbt seed --full-refresh --target snowflake
dbt run-operation prep_external --target snowflake
dbt run-operation dbt_external_tables.stage --target snowflake --per-node --select source:snowflake_external.people_csv_unpartitioned source:snowflake_external.people_csv_partitioned --threads 2 --vars 'ext_full_refresh: true'
dbt run-operation dbt_external_tables.stage --target snowflake --per-node --select source:snowflake_external.people_csv_unpartitioned source:snowflake_external.people_csv_partitioned --threads 2
dbt test --target snowflake --select source:snowflake_external.people_csv_unpartitioned source:snowflake_external.people_csv_partitioned
```

The first staging call creates or replaces both tables. The second refreshes their metadata. Each invocation should report two successful source results. The tests compare the staged data with the `people` seed.
