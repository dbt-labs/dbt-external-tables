# dbt-external-tables v0.8.0

This release supports any version (minor and patch) of v1, which means far less need for compatibility releases in the future.

### Features
- (Snowflake) Support for regex `pattern` in snowpipes ([#111](https://github.com/dbt-labs/dbt-external-tables/pull/111), [#122](https://github.com/dbt-labs/dbt-external-tables/pull/122))
- (Apache Spark) Real support for partitioned external tables. Note that external sources with `partitions` defined were implicitly skipped. Going forward, sources with partitions defined (excluding those with `using: delta`) will run `alter table ... recover partitions`.

### Under the hood
- Use standard logging, thereby removing dependency on `dbt_utils` ([#119](https://github.com/dbt-labs/dbt-external-tables/pull/119))
- Remove `synapse__`-prefixed "passthrough" macros, now that `dbt-synapse` can use `sqlserver__`-prefixed macros instead ([#110](https://github.com/dbt-labs/dbt-external-tables/pull/110))

### Contributors
- [@JCZuurmond](https://github.com/JCZuurmond) ([#116](https://github.com/dbt-labs/dbt-external-tables/pull/116))
- [@stumelius](https://github.com/stumelius) ([#111](https://github.com/dbt-labs/dbt-external-tables/pull/111))
- [@swanderz](https://github.com/swanderz) ([#110](https://github.com/dbt-labs/dbt-external-tables/pull/110))

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
