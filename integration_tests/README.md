## Integration tests

The files in `public_data` are available in two public storage buckets:
- `s3://dbt-external-tables-testing`
- `gs://dbt-external-tables-testing/`

These integration tests confirm that, when staged as external tables, using different databases / file formats / partitioning schemes, the final combined output is equivalent to `seeds/people.csv`.
