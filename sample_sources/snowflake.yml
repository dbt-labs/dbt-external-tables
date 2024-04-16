version: 2

sources:
  - name: snowplow
    database: analytics
    schema: snowplow_external
    loader: S3
    loaded_at_field: collector_hour
    
    tables:
      - name: event_ext_tbl
        description: "External table of Snowplow events stored as JSON files"
        external:
          location: "@raw.snowplow.snowplow"  # reference an existing external stage
          file_format: "( type = json )"      # fully specified here, or reference an existing file format
          auto_refresh: true                  # requires configuring an event notification from Amazon S3 or Azure
          partitions:
            - name: collector_hour
              data_type: timestamp
              expression: to_timestamp(substr(metadata$filename, 8, 13), 'YYYY/MM/DD/HH24')
              
        # all Snowflake external tables natively include a `metadata$filename` pseudocolumn
        # and a `value` column (JSON blob-ified version of file contents), so there is no need to specify
        # them here. you may optionally specify columns to unnest or parse from the file:
        columns:
          - name: app_id
            data_type: varchar(255)
            description: "Application ID"
          - name: domain_sessionidx
            data_type: int
            description: "A visit / session index"
          - name: etl_tstamp
            data_type: timestamp
            description: "Timestamp event began ETL"
          - name: etl timestamp
            # Use double-quoted identifiers for name and identifier
            quote: true
            # Specifying alias lets us rename etl timestamp to "etl_timestamp"
            alias: etl_timestamp
            data_type: timestamp
            description: "Timestamp event began ETL with a double quoted identifier"
          - name: etl_date
            data_type: date
            description: "Date event began ETL"
            # Expressions can manipulate the variant value prior to casting to data_type.
            expression: TRY_TO_DATE(VALUE:etl_tstamp::VARCHAR, 'YYYYMMDD')
          - name: contexts
            data_type: variant
            description: "Contexts attached to event by Tracker"
        
              
      - name: event_snowpipe
        description: "Table of Snowplow events, stored as JSON files, loaded in near-real time via Snowpipe"
        loader: S3 + snowpipe    # this is just for your reference
        external:
          location: "@raw.snowplow.snowplow"
          file_format: "{{ target.schema }}.my_json_file_format"
          pattern: ".*[.]json"  # Optional object key pattern

          # Instead of an external tables, create an empty table, backfill it, and pipe new data
          snowpipe:
            auto_ingest:    true  # requires either `aws_sns_topic` or `integration`
            aws_sns_topic:  # Amazon S3
            integration:    # Google Cloud or Azure
            copy_options:   "on_error = continue, enforce_length = false" # e.g.
              
        # dbt will include three metadata columns in addition to any `columns`
        # specified for a snowpiped table:
        #   `metadata_filename`: the file from which this row was loaded
        #   `metadata_file_row_number`: the numbered row this was in that file
        #   `_dbt_copied_at`: the current_timestamp when this row was loaded (backfilled or piped)
        #
        # if you do not specify *any* columns for a snowpiped table, dbt will also
        # include `value`, the JSON blob of all file contents.
        
      - name: delta_tbl
        description: "External table using Delta files"
        external:
          location: "@stage"                  # reference an existing external stage
          file_format: "( type = parquet )"   # fully specified here, or reference an existing file format
          table_format: delta                 # specify the table format
          auto_refresh: false                  # requires configuring an event notification from Amazon S3 or Azure


      - name: parquet_with_inferred_schema
        description: "External table using Parquet and inferring the schema"
        external:
          location: "@stage"                  # reference an existing external stage
          file_format: "my_file_format"       # we need a named file format for infer to work
          infer_schema: true                  # parameter to tell Snowflake we want to infer the table schema
          partitions:
            - name: section                   # we can define partitions on top of the schema columns
              data_type: varchar(64)
              expression: "substr(split_part(metadata$filename, 'section=', 2), 1, 1)"
        columns:                              # columns can still be listed for documentation/testing purpose
          - name: id
            description: this is an id
          - name: name
            description: and this is a name

      - name: aws_sns_refresh_tbl
        description: "External table using AWS SNS for auto-refresh"
        external:
          location: "@stage"                  # reference an existing external stage
          file_format: "( type = csv )"   
          # auto_refresh is assumed, setting to false is not supported
          aws_sns_topic: "arn:aws:sns:us-east-1:123456789012:my_topic" # SNS topic ARN