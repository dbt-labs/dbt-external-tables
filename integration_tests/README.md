## Integration tests

The files in `public_data` are available in two public storage buckets:
- `s3://dbt-external-tables-testing`
- `gs://dbt-external-tables-testing/`

The integration tests stage external files and compare the results with `seeds/people.csv`.

### Run locally

Use an isolated warehouse schema and a dbt v2 (Fusion) build with `run-operation --per-node`. For BigQuery, the build must also support operation plans with `sql` and `post_macro` keys. Set the connection environment variables from [`profiles.yml`](profiles.yml), or point `DBT_PROFILES_DIR` at a directory containing a compatible test profile. Run from the package root.

```bash
DBT_TEST_BIN=/path/to/dbt ./run_test.sh snowflake
DBT_TEST_BIN=/path/to/dbt ./run_test.sh bigquery
DBT_TEST_BIN=/path/to/dbt ./run_test.sh redshift
```

The script checks for `--per-node` before accessing the warehouse. It installs package dependencies, seeds expected data, and prepares the fixture resources. It then stages `source:*` with `ext_full_refresh: true`, stages again to exercise the normal refresh path, and runs `dbt test`. By default staging uses two threads. Set `DBT_TEST_THREADS` to change the limit.

### Run in GitHub Actions

Set `DBT_FUSION_VERSION` to an exact published dbt v2 version that supports the operation plans used by this package, or supply that version when dispatching the Integration Testing workflow. Configure the warehouse credentials referenced in the workflow. Each matrix job provisions the pinned CLI and runs the same script. Until a compatible version is published, use a provisioned development binary for local testing.
