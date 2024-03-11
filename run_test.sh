#!/bin/bash

echo "Setting up virtual environment for dbt-$1"
echo "Changing working directory: integration_tests"
cd integration_tests

if [[ ! -e ~/.dbt/profiles.yml ]]; then
    echo "Copying sample profile"
    mkdir -p ~/.dbt
    cp ci/sample.profiles.yml ~/.dbt/profiles.yml
fi

echo "Starting integration tests"
set -eo pipefail
dbt deps --target $1
dbt seed --full-refresh --target $1
dbt run-operation prep_external --target $1
dbt run-operation dbt_external_tables.stage_external_sources --vars 'ext_full_refresh: true' --target $1
dbt run-operation dbt_external_tables.stage_external_sources --target $1
dbt test --target $1
