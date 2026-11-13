"""Render Fabric DDL using dbt's parsed source/test metadata layout."""
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import pytest
from jinja2 import Environment, StrictUndefined

SOURCE_ID = "source.fixture.sample.example"
SOURCE = {
    "unique_id": SOURCE_ID,
    "source_name": "sample",
    "name": "example",
    "external": {"location": "/tmp/example", "ansi_nulls": None, "quoted_identifier": None},
    "columns": {
        "required_id": {"name": "required_id", "data_type": "int"},
        "optional_id": {"name": "optional_id", "data_type": "int"},
    },
}
TEST = {
    "resource_type": "test",
    "column_name": "required_id",
    "test_metadata": {"name": "not_null", "namespace": None},
    "depends_on": {"nodes": [SOURCE_ID]},
    "config": {"enabled": True, "where": None},
}


def render(test, execute=True):
    path = Path(__file__).resolve().parents[2] / "macros/plugins/fabric/create_external_table.sql"
    module = Environment(undefined=StrictUndefined, extensions=["jinja2.ext.do"]).from_string(path.read_text()).make_module({
        "graph": {"nodes": {"test.fixture": test}},
        "execute": execute,
        "source": lambda *args: "sample.example",
        "adapter": SimpleNamespace(quote=lambda name: f'[{name}]'),
    })
    return module.fabric__create_external_table(SOURCE)


def test_required_and_optional_columns():
    sql = render(TEST)
    assert "[required_id] int NOT NULL" in sql
    assert "[optional_id] int NULL" in sql


@pytest.mark.parametrize("case", ["filtered", "disabled", "other_source", "custom_test", "singular", "parse"])
def test_non_applicable_tests_do_not_constrain_column(case):
    test = deepcopy(TEST)
    if case == "filtered":
        test["config"]["where"] = "required_id > 0"
    elif case == "disabled":
        test["config"]["enabled"] = False
    elif case == "other_source":
        test["depends_on"]["nodes"] = ["source.other"]
    elif case == "custom_test":
        test["test_metadata"]["namespace"] = "custom_package"
    elif case == "singular":
        test["test_metadata"] = None
    assert "[required_id] int NULL" in render(test, execute=case != "parse")


def test_quoted_column_name():
    test = deepcopy(TEST)
    test["column_name"] = "[required_id]"
    assert "[required_id] int NOT NULL" in render(test)
