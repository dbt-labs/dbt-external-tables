#!/bin/bash
set -eo pipefail

echo "Setting up virtual environment"
VENV="venv/bin/activate"

if [[ ! -f $VENV ]]; then
    # sudo apt update
    # sudo apt install python3.8-venv
    # python3.8 -m venv venv
    # . $VENV
    pip3 install --upgrade pip setuptools
    if [ $1 == 'databricks' ]
    then
        echo "Installing dbt-spark"
        pip3 install dbt-spark[ODBC] --upgrade --pre
    elif [ $1 == 'azuresql' ]
    then
        echo "Installing dbt-sqlserver"
        pip3 install dbt-sqlserver --upgrade --pre
    else
        echo "Installing dbt-$1"
        pip3 install dbt-$1 --upgrade --pre
    fi
fi

# . $VENV
echo "Changing working directory: integration_tests"
cd integration_tests

if [[ ! -e ~/.dbt/profiles.yml ]]; then
    echo "Copying sample profile"
    mkdir -p ~/.dbt
    cp ci/sample.profiles.yml ~/.dbt/profiles.yml
fi

echo "Starting integration tests"
dbt deps --target $1
dbt seed --full-refresh --target $1
dbt run-operation prep_external --target $1
dbt run-operation stage_external_sources --vars 'ext_full_refresh: true' --target $1
dbt run-operation stage_external_sources --target $1
dbt test --target $1
