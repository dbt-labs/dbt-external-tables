# Changelog

## dbt-external-tables v0.11.0

### Synapse & SQL Server
* Reenable sqlserver and synapse support https://github.com/dbt-labs/dbt-external-tables/pull/332


**Full Changelog**: https://github.com/dbt-labs/dbt-external-tables/compare/0.10.1...0.10.0

## dbt-external-tables v0.10.1

* [FIX] OOPS! Revert https://github.com/dbt-labs/dbt-external-tables/pull/330 "stage_external_sources Comparing source_name of the node instead of the name of the node" by @dataders in https://github.com/dbt-labs/dbt-external-tables/pull/330
* Update CI trigger to run off forks by @emmyoop in https://github.com/dbt-labs/dbt-external-tables/pull/329


**Full Changelog**: https://github.com/dbt-labs/dbt-external-tables/compare/0.10.1...0.10.0

## BROKEN dbt-external-tables v0.10.0

DO NOT USE THIS VERSION. USE `v0.10.1` or higher.

### Snowflake
* Refactor create_external_table.sql in snowflake plugin by @kyleburke-meq in https://github.com/dbt-labs/dbt-external-tables/pull/318
* stage_external_sources Comparing source_name of the node instead of the name of the node by @ward-resa in https://github.com/dbt-labs/dbt-external-tables/pull/312
* added ignore_case for Snowflake by @cakkinep in https://github.com/dbt-labs/dbt-external-tables/pull/308

## New Contributors
* @ward-resa made their first contribution in https://github.com/dbt-labs/dbt-external-tables/pull/312

**Full Changelog**: https://github.com/dbt-labs/dbt-external-tables/compare/0.9.0...0.10.0

## dbt-external-tables v0.9.0

### Snowflake
* Add metadata_file_last_modified for snowpiped tables by @Catisyf in https://github.com/dbt-labs/dbt-external-tables/pull/239
* snowflake delta format by @danielefrigo in https://github.com/dbt-labs/dbt-external-tables/pull/240
* Support aws_sns_topic property in Snowflake by @jtmcn in https://github.com/dbt-labs/dbt-external-tables/pull/243
* alias column for snowflake external table by @cakkinep in https://github.com/dbt-labs/dbt-external-tables/pull/257
* Snowflake: Add expression parameter to columns by @kyleburke-meq @jpear3 in https://github.com/dbt-labs/dbt-external-tables/pull/275

### BigQuery
* Handle BigQuery non-string option 'max_staleness' by @marcbllv in https://github.com/dbt-labs/dbt-external-tables/pull/237
* quote project name by @thomas-vl in https://github.com/dbt-labs/dbt-external-tables/pull/242
* update external table columns by @thomas-vl in https://github.com/dbt-labs/dbt-external-tables/pull/252

### under the hood
* Fix protobuf v5 issue in CI by @thomas-vl in https://github.com/dbt-labs/dbt-external-tables/pull/258
* move to GitHub Actions by @dataders in https://github.com/dbt-labs/dbt-external-tables/pull/265
* Rebase test by @dataders in https://github.com/dbt-labs/dbt-external-tables/pull/273
* run workflow in context of base repo by @dataders in https://github.com/dbt-labs/dbt-external-tables/pull/278
* actual test case for #257 by @dataders in https://github.com/dbt-labs/dbt-external-tables/pull/290

## New Contributors
* @marcbllv made their first contribution in https://github.com/dbt-labs/dbt-external-tables/pull/237
* @Catisyf made their first contribution in https://github.com/dbt-labs/dbt-external-tables/pull/239
* @danielefrigo made their first contribution in https://github.com/dbt-labs/dbt-external-tables/pull/240
* @jtmcn made their first contribution in https://github.com/dbt-labs/dbt-external-tables/pull/243
* @cakkinep made their first contribution in https://github.com/dbt-labs/dbt-external-tables/pull/257
* @kyleburke-meq made their first contribution in https://github.com/dbt-labs/dbt-external-tables/pull/275
* @jpear3 made their first contribution in https://github.com/dbt-labs/dbt-external-tables/pull/275

**Full Changelog**: https://github.com/dbt-labs/dbt-external-tables/compare/0.8.7...0.9.0

## dbt-external-tables v0.8.0

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

## dbt-external-tables v0.7.3

### Fixes
- Hard code printer width for backwards compatibility with older versions of dbt Core ([#120](https://github.com/dbt-labs/dbt-external-tables/pull/120))

## dbt-external-tables v0.7.2
🚨 This is a compatibility release in preparation for `dbt-core` v1.0.0 (🎉). Projects using this version with `dbt-core` v1.0.x can expect to see a deprecation warning. This will be resolved in the next minor release.

### Fixes
- (BigQuery) Fix `create external tables` with multiple partitions, by including missing comma ([#114](https://github.com/dbt-labs/dbt-external-tables/pull/114))
- (Snowflake) Fix `auto_refresh` when not specified `False` ([#117](https://github.com/dbt-labs/dbt-external-tables/pull/117))

### Contributors
- [@stumelius](https://github.com/stumelius) ([#114](https://github.com/dbt-labs/dbt-external-tables/pull/114))
