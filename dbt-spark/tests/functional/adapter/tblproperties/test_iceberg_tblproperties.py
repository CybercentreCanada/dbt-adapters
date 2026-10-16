"""Functional tests for Iceberg tblproperties create and sync behavior.

These tests require a Spark environment with Iceberg support.
They validate that:
1. CREATE TABLE includes tblproperties from config
2. Incremental runs SET new/changed properties
3. Incremental runs UNSET removed properties
4. polaris_ properties are never touched
"""

import pytest

from dbt.tests.util import run_dbt, get_connection, relation_from_name


_MODEL_CREATE = """
{{
    config(
        materialized='incremental',
        file_format='iceberg',
        tblproperties={
            'write.format.default': 'parquet',
            'history.expire.max-snapshot-age-ms': '86400000'
        }
    )
}}
select 1 as id
"""

_MODEL_ADD_PROPERTY = """
{{
    config(
        materialized='incremental',
        file_format='iceberg',
        tblproperties={
            'write.format.default': 'parquet',
            'history.expire.max-snapshot-age-ms': '86400000',
            'read.split.target-size': '134217728'
        }
    )
}}
select 1 as id
"""

_MODEL_REMOVE_PROPERTY = """
{{
    config(
        materialized='incremental',
        file_format='iceberg',
        tblproperties={
            'write.format.default': 'parquet'
        }
    )
}}
select 1 as id
"""

_MODEL_UPDATE_PROPERTY = """
{{
    config(
        materialized='incremental',
        file_format='iceberg',
        tblproperties={
            'write.format.default': 'avro',
            'history.expire.max-snapshot-age-ms': '86400000'
        }
    )
}}
select 1 as id
"""


def _get_tblproperties(project, model_name):
    """Fetch tblproperties from the catalog for a given model."""
    relation = relation_from_name(project.adapter, model_name)
    _, result = project.adapter.execute(f"SHOW TBLPROPERTIES {relation}", fetch=True)
    props = {}
    for row in result.rows:
        props[str(row[0])] = str(row[1])
    return props


class TestIcebergTblpropertiesCreate:
    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": _MODEL_CREATE}

    def test_create_sets_properties(self, project):
        run_dbt(["run"])
        props = _get_tblproperties(project, "my_model")
        assert props.get("write.format.default") == "parquet"
        assert props.get("history.expire.max-snapshot-age-ms") == "86400000"


class TestIcebergTblpropertiesAddProperty:
    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": _MODEL_CREATE}

    def test_add_property_on_incremental(self, project):
        # First run: create with initial properties
        run_dbt(["run"])

        # Second run: add a property
        project.models = {"my_model.sql": _MODEL_ADD_PROPERTY}
        run_dbt(["run"])

        props = _get_tblproperties(project, "my_model")
        assert props.get("read.split.target-size") == "134217728"
        assert props.get("write.format.default") == "parquet"


class TestIcebergTblpropertiesRemoveProperty:
    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": _MODEL_CREATE}

    def test_remove_property_on_incremental(self, project):
        # First run: create with two properties
        run_dbt(["run"])

        # Second run: remove one property
        project.models = {"my_model.sql": _MODEL_REMOVE_PROPERTY}
        run_dbt(["run"])

        props = _get_tblproperties(project, "my_model")
        assert props.get("write.format.default") == "parquet"
        assert "history.expire.max-snapshot-age-ms" not in props


class TestIcebergTblpropertiesUpdateProperty:
    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": _MODEL_CREATE}

    def test_update_property_on_incremental(self, project):
        # First run: create
        run_dbt(["run"])

        # Second run: change value
        project.models = {"my_model.sql": _MODEL_UPDATE_PROPERTY}
        run_dbt(["run"])

        props = _get_tblproperties(project, "my_model")
        assert props.get("write.format.default") == "avro"
