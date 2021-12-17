# dbt-external-tables v0.7.3

### Fixes
- Hard code printer width for backwards compatibility with older versions of dbt Core ([#120](https://github.com/dbt-labs/dbt-external-tables/pull/120))

# dbt-external-tables v0.7.2
🚨 This is a compatibility release in preparation for `dbt-core` v1.0.0 (🎉). Projects using this version with `dbt-core` v1.0.x can expect to see a deprecation warning. This will be resolved in the next minor release.

### Fixes
- (BigQuery) Fix `create external tables` with multiple partitions, by including missing comma ([#114](https://github.com/dbt-labs/dbt-external-tables/pull/114))
- (Snowflake) Fix `auto_refresh` when not specified `False` ([#117](https://github.com/dbt-labs/dbt-external-tables/pull/117))

### Contributors
- [@stumelius](https://github.com/stumelius) ([#114](https://github.com/dbt-labs/dbt-external-tables/pull/114))
