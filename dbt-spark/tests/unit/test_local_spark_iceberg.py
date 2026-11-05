from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import yaml
import agate

from dbt.adapters.spark import SparkAdapter, SparkRelation
from dbt_common.exceptions import DbtRuntimeError

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib

from tests.functional.conftest import dbt_project_yml, project
from tests.functional.fixtures.profiles import local_spark_iceberg_target
from tests.functional.adapter.tblproperties.test_iceberg_tblproperties import (
    _get_tblproperties,
)


SPARK_ROOT = Path(__file__).resolve().parents[2]


def request_for(profile):
    return SimpleNamespace(config=SimpleNamespace(getoption=lambda _: profile))


def test_hatch_environment_installs_project_without_default_extras():
    config = tomllib.loads((SPARK_ROOT / "hatch.toml").read_text())
    env = config["envs"]["local-spark-iceberg"]
    assert env["template"] == "local-spark-iceberg"
    assert not env.get("detached", False)
    assert env["skip-install"] is False
    assert env["features"] == ["session"]
    assert "pyspark==4.1.3" in env["dependencies"]
    assert "pip install -e ../dbt-tests-adapter" in env["pre-install-commands"]


def test_profile_uses_view_capable_rest_catalog_and_loopback():
    parameters = local_spark_iceberg_target()["server_side_parameters"]
    assert parameters["spark.driver.host"] == "127.0.0.1"
    assert parameters["spark.driver.bindAddress"] == "127.0.0.1"
    assert parameters["spark.sql.catalog.iceberg.type"] == "rest"
    assert parameters["spark.sql.catalog.iceberg.uri"] == "http://127.0.0.1:8181"
    assert parameters["spark.jars.packages"] == (
        "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.12.0"
    )
    catalog = yaml.safe_load(
        (SPARK_ROOT / "docker" / "compose.local-spark-iceberg.yml").read_text()
    )["services"]["catalog"]
    assert catalog["ports"] == ["127.0.0.1:8181:8181"]
    assert catalog["environment"]["CATALOG_JDBC_SCHEMA__VERSION"] == "V1"
    assert catalog["volumes"][0]["source"] == catalog["volumes"][0]["target"]


@pytest.mark.parametrize("profile", ["local_spark_iceberg", "apache_spark"])
def test_project_defaults_are_local_only_and_preserve_overrides(tmp_path, profile):
    config = {
        "name": "test",
        "flags": {"send_anonymous_usage_stats": False},
        "models": {"test": {"+tblproperties": {"custom": "value"}}},
    }
    result = dbt_project_yml.__wrapped__(config, request_for(profile), tmp_path)
    assert result["models"]["test"]["+tblproperties"] == {"custom": "value"}
    if profile == "local_spark_iceberg":
        assert result["models"]["+database"] == "iceberg"
        assert result["models"]["+file_format"] == "iceberg"
        assert result["flags"]["require_location_root"] is False
        assert yaml.safe_load((tmp_path / "dbt_project.yml").read_text()) == result
    else:
        assert "+database" not in result["models"]
        assert "require_location_root" not in result["flags"]
        assert not (tmp_path / "dbt_project.yml").exists()


@pytest.mark.parametrize("profile", ["local_spark_iceberg", "apache_spark"])
def test_relation_cleanup_runs_only_for_local_catalog(profile):
    adapter = Mock()
    adapter.connection_named.return_value.__enter__ = Mock()
    adapter.connection_named.return_value.__exit__ = Mock(return_value=False)
    relations = [Mock(), Mock()]
    adapter.list_relations_without_caching.return_value = relations
    info = SimpleNamespace(adapter=adapter, database=None, test_schema="test_schema")
    fixture = project.__wrapped__(info, request_for(profile))
    assert next(fixture) is info
    with pytest.raises(StopIteration):
        next(fixture)
    if profile == "local_spark_iceberg":
        assert info.database == "iceberg"
        assert adapter.drop_relation.call_count == 2
        for relation in relations:
            adapter.drop_relation.assert_any_call(relation)
    else:
        adapter.connection_named.assert_not_called()
        assert info.database is None


def test_tblproperties_query_acquires_connection(monkeypatch):
    adapter = Mock()
    adapter.connection_named.return_value.__enter__ = Mock()
    adapter.connection_named.return_value.__exit__ = Mock(return_value=False)
    adapter.execute.return_value = (None, SimpleNamespace(rows=[("custom", "value")]))
    monkeypatch.setattr(
        "tests.functional.adapter.tblproperties.test_iceberg_tblproperties.relation_from_name",
        lambda *_: "iceberg.schema.model",
    )
    assert _get_tblproperties(SimpleNamespace(adapter=adapter), "model") == {"custom": "value"}
    adapter.connection_named.assert_called_once_with("tblproperties")
    adapter.execute.assert_called_once_with("SHOW TBLPROPERTIES iceberg.schema.model", fetch=True)


@pytest.mark.parametrize("v1_fallback", [False, True])
@pytest.mark.parametrize("iceberg_catalog", [False, True])
def test_describe_listing_includes_persistent_iceberg_views(
    monkeypatch, v1_fallback, iceberg_catalog
):
    adapter = object.__new__(SparkAdapter)
    adapter._behavior = SimpleNamespace(spark_table_only=False)
    schema = SparkRelation.create(database="iceberg", schema="test_schema")
    rows = agate.Table(
        [("test_schema", "orders", False), ("", "temp", True)],
        ["namespace", "tableName", "isTemporary"],
    )
    views = agate.Table(
        [("test_schema", "report", False), ("", "orders", True)],
        ["namespace", "viewName", "isTemporary"],
    )
    macro = Mock(return_value=rows)
    if v1_fallback:
        macro.side_effect = [
            DbtRuntimeError("SHOW TABLE EXTENDED is not supported for v2 tables"),
            rows,
        ]
    sql = Mock(return_value=(None, views))
    monkeypatch.setattr(adapter, "execute_macro", macro)
    monkeypatch.setattr(adapter, "execute", sql)
    monkeypatch.setattr(adapter, "_catalog_is_iceberg_only", lambda _: iceberg_catalog)
    monkeypatch.setattr(
        adapter,
        "_get_relation_information_using_describe",
        lambda row, _: (row[0], row[1], "Provider: iceberg" if row[1] == "orders" else ""),
    )
    listing = adapter._list_relations_v1_first if v1_fallback else adapter._list_relations_v2_first
    result = listing({"schema_relation": schema}, schema)
    by_name = {relation.identifier: relation for relation in result}
    assert by_name["orders"].is_table
    assert by_name["orders"].is_iceberg
    if iceberg_catalog:
        assert set(by_name) == {"orders", "report"}
        assert by_name["report"].is_view
        assert not by_name["report"].is_iceberg
        sql.assert_called_once_with(f"show views in {schema} like '*'", fetch=True)
    else:
        assert set(by_name) == {"orders", "temp"}
        sql.assert_not_called()
