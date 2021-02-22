#!/bin/bash
VENV="venv/bin/activate"

if [[ ! -f $VENV ]]; then
    python3 -m venv venv
    . $VENV
    pip install --upgrade pip setuptools
    if [ $1 == 'databricks' ]
    then
        pip install --pre dbt-spark[ODBC] --upgrade
    elif [ $1 == 'synapse' ]
    then
        pip install --pre dbt-synapse>=0.19.0 --upgrade
    elif [ $1 == 'azuresql' ]
    then
        pip install --pre dbt-sqlserver --upgrade
    else
        pip install --pre dbt --upgrade
    fi
fi

. $VENV
cd integration_tests

if [[ ! -e ~/.dbt/profiles.yml ]]; then
    mkdir -p ~/.dbt
    cp ci/sample.profiles.yml ~/.dbt/profiles.yml
fi

dbt deps --target $1
dbt seed --full-refresh --target $1
dbt run-operation prep_external --target $1
dbt run-operation stage_external_sources --var 'ext_full_refresh: true' --target $1
dbt run-operation stage_external_sources --target $1
dbt test --target $1
