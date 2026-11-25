#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 1 ]]; then
    printf 'Usage: %s TARGET\n' "${0##*/}" >&2
    exit 2
fi

dbt_test_target=$1
package_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
dbt_binary=${DBT_TEST_BIN:-dbt}
if ! dbt_binary=$(command -v -- "$dbt_binary"); then
    printf 'Set DBT_TEST_BIN to an executable dbt v2 binary.\n' >&2
    exit 2
fi
if [[ $dbt_binary != /* ]]; then
    dbt_binary="$PWD/$dbt_binary"
fi

if ! operation_help=$("$dbt_binary" run-operation --help) || [[ $operation_help != *--per-node* ]]; then
    printf 'Integration tests require a dbt v2 build with run-operation --per-node. Set DBT_TEST_BIN to a compatible binary.\n' >&2
    exit 2
fi
"$dbt_binary" --version

profiles_dir=${DBT_PROFILES_DIR:-$package_root/integration_tests}
if [[ $profiles_dir != /* ]]; then
    profiles_dir="$PWD/$profiles_dir"
fi
cd -- "$package_root/integration_tests"

target_args=(--target "$dbt_test_target" --profiles-dir "$profiles_dir")
stage_args=(--per-node --select 'source:*' --threads "${DBT_TEST_THREADS:-2}")

"$dbt_binary" deps "${target_args[@]}"
"$dbt_binary" seed --full-refresh "${target_args[@]}"
"$dbt_binary" run "${target_args[@]}"
"$dbt_binary" run-operation prep_external "${target_args[@]}"
"$dbt_binary" run-operation dbt_external_tables.stage "${stage_args[@]}" --vars 'ext_full_refresh: true' "${target_args[@]}"
"$dbt_binary" run-operation dbt_external_tables.stage "${stage_args[@]}" "${target_args[@]}"
"$dbt_binary" test "${target_args[@]}"
