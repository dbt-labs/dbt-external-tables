### External tables in dbt

* Source config extension for metadata about external file structure
* Adapter macros to create and "refresh" partitioned external tables

```bash
# iterate through all source nodes, run drop + create + refresh (if partitioned)
dbt run-operation stage_external_sources
# maybe someday: dbt source create-external ?
```

The macros assume that you have already created an external stage (Snowflake)
or external schema (Spectrum), and that you have permissions to select from it
and create tables in it.

### Spec

```yml
source:
  - name: snowplow
    tables:
      - name: event
      
        # NEW: "external" property of source node
        external:
          location: # S3 file path or stage (Snowflake)
          file_format: # Hive or Snowflake
          row_format: # Hive
          tbl_properties: # Hive
          
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
              vals:     # array of values
                - dev
                - prod
              # macro to convert partition value to file path specification
              # takes keyword arguments 'name' + 'value'
              path_macro: test_external_sources_redshift.year_month_day
            
            - name: collector_date
              data_type: date
              vals:     # macro w/ args to generate array of values
                macro: dbt.dates_in_range   
                args:
                  start_date_str: '2019-08-01'
                  end_date_str: '{{modules.datetime.date.today().strftime("%Y-%m-%d")}}'
                  in_fmt: "%Y-%m-%d"
                  out_fmt: "%Y-%m-%d"
               path_macro: test_external_sources_redshift.year_month_day
             
        
        # Specify ALL column names + datatypes
        columns:
          - name: app_id
            data_type: varchar(255)
            description: "Application ID"
        ...
```

See `sample_sources` for full, valid YML config that establishes Snowplow events
as a dbt source and stage-ready external table in Snowflake and Spectrum.

### Current dependencies

* dbt@0.15.0 in [`dev/louisa-may-alcott`](https://github.com/fishtown-analytics/dbt/tree/dev/louisa-may-alcott)

### Supported databases

* Redshift (Spectrum)
* Snowflake
* TK: Spark
