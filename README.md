# External sources in dbt

* Source config extension for metadata about external file structure
* Adapter macros to create external tables and refresh external table partitions
* Snowflake-specific macros to create, backfill, and refresh snowpipes

## Syntax

```bash
# iterate through all source nodes, create if missing + refresh if appropriate
$ dbt run-operation stage_external_sources

# iterate through all source nodes, create or replace + refresh if appropriate
$ dbt run-operation stage_external_sources --vars 'ext_full_refresh: true'
```

![sample docs](etc/sample_docs.png)

The macros assume that you have already created an external stage (Snowflake)
or external schema (Redshift/Spectrum), and that you have permissions to select from it
and create tables in it.

The `stage_external_sources` macro accepts a similar node selection syntax to
[snapshotting source freshness](https://docs.getdbt.com/docs/running-a-dbt-project/command-line-interface/source/#specifying-sources-to-snapshot).

```bash
# Stage all Snowplow and Logs external sources:
$ dbt run-operation stage_external_sources --args 'select: snowplow logs'

# Stage a particular external source table:
$ dbt run-operation stage_external_sources --args 'select: snowplow.event'
```

Maybe someday:
```bash
$ dbt source stage-external
$ dbt source stage-external --full-refresh
$ dbt source stage-external --select snowplow.event logs
```

## Spec

```yml
version: 2

sources:
  - name: snowplow
    tables:
      - name: event
      
                            # NEW: "external" property of source node
        external:
          location:         # S3 file path or Snowflake stage
          file_format:      # Hive specification or Snowflake named format / specification
          row_format:       # Hive specification
          table_properties: # Hive specification
          
          # Snowflake: create an empty table + pipe instead of an external table
          snowpipe:
            auto_ingest:    true
            aws_sns_topic:  # AWS
            integration:    # Azure
            copy_options:   "on_error = continue, enforce_length = false" # e.g.
          
                            # Specify a list of file-path partitions.
          
          # ------ SNOWFLAKE ------
          partitions:
            - name: collector_date
              data_type: date
              expression: to_date(substr(metadata$filename, 8, 10), 'YYYY/MM/DD')
              
          # ------ REDSHIFT -------
          partitions:
            - name: appId
              data_type: varchar(255)
              vals:         # list of values
                - dev
                - prod
              path_macro: dbt_external_tables.key_value
                  # Macro to convert partition value to file path specification.
                  # This "helper" macro is defined in the package, but you can use
                  # any custom macro that takes keyword arguments 'name' + 'value'
                  # and returns the path as a string
            
                  # If multiple partitions, order matters for compiling S3 path
            - name: collector_date
              data_type: date
              vals:         # macro w/ keyword args to generate list of values
                macro: dbt.dates_in_range
                args:
                  start_date_str: '2019-08-01'
                  end_date_str: '{{modules.datetime.date.today().strftime("%Y-%m-%d")}}'
                  in_fmt: "%Y-%m-%d"
                  out_fmt: "%Y-%m-%d"
               path_macro: dbt_external_tables.year_month_day
             
        
        # Specify ALL column names + datatypes. Column order matters for CSVs. 
        # Other file formats require column names to exactly match.
        
        columns:
          - name: app_id
            data_type: varchar(255)
            description: "Application ID"
          - name: platform
            data_type: varchar(255)
            description: "Platform"
        ...
```

## Resources

* [`sample_sources`](sample_sources) for full valid YML config that establishes Snowplow events
as a dbt source and stage-ready external table in Snowflake and Spectrum.
* [`sample_analysis`](sample_analysis) for a "dry run" version of the DDL/DML that
`stage_external_sources` will run as an operation

## Supported databases

* Redshift (Spectrum)
* Snowflake
* TK: Spark
