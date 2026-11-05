import sys
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import Mock
from pathlib import Path
from jinja2 import Environment

import pytest

from dbt.adapters.spark import SparkAdapter, SparkRelation
from dbt.adapters.spark.functions import SparkFunction, SparkFunctionRegistry
from dbt.adapters.spark.session import Cursor
from dbt_common.exceptions import DbtRuntimeError


def function_model(language="sql", name="double_value", **configs):
    return {
        "unique_id": f"function.test.{name}",
        "resource_type": "function",
        "name": name,
        "identifier": name,
        "language": language,
        "config": {
            "type": "scalar",
            "spark_function_scope": "session",
            "runtime_version": f"{sys.version_info.major}.{sys.version_info.minor}",
            "entry_point": "implementation",
            **configs,
        },
        "arguments": [{"name": "value", "data_type": "INT"}],
        "returns": {"data_type": "INT"},
        "compiled_code": "value * 2" if language == "sql" else "return value * 2",
        "depends_on": {"nodes": []},
    }


def session_mock():
    session = Mock()
    session.version = "4.1.3"
    session.conf.get.return_value = "false"
    session.catalog.functionExists.return_value = False
    return session


@pytest.mark.parametrize(
    "config,message",
    [
        ({"spark_function_scope": None}, "spark_function_scope"),
        ({"type": "aggregate"}, "scalars"),
        ({"volatility": "stable"}, "volatility"),
        ({"grants": {"execute": ["someone"]}}, "grants"),
        ({"persist_docs": {"relation": True}}, "documentation"),
        ({"runtime_version": "0.0"}, "runtime_version"),
        ({"entry_point": "class"}, "identifier"),
    ],
)
def test_invalid_configuration(config, message):
    with pytest.raises(DbtRuntimeError, match=message):
        SparkFunction.from_model(function_model("python", **config))


@pytest.mark.parametrize(
    "field,value,message",
    [
        ("arguments", [{"name": "bad-name", "data_type": "INT"}], "identifier"),
        ("arguments", [{"name": "value", "data_type": "INT", "default_value": "1"}], "defaults"),
        ("arguments", [{"name": "x", "data_type": "INT"}] * 2, "Duplicate"),
        ("compiled_code", "return (", "Invalid Python"),
        ("compiled_code", "", "compiled body"),
        ("returns", {}, "return type"),
    ],
)
def test_invalid_python_contract(field, value, message):
    model = function_model("python")
    model[field] = value
    with pytest.raises(DbtRuntimeError, match=message):
        SparkFunction.from_model(model)


def test_python_wrapper_does_not_execute_body():
    model = function_model("python")
    model["compiled_code"] = "raise RuntimeError('only on invocation')"
    function = SparkFunction.from_model(model)
    namespace = {}
    exec(function.python_code(), namespace)
    assert "spark" not in namespace and "dbt" not in namespace
    with pytest.raises(RuntimeError, match="only on invocation"):
        namespace["implementation"](1)


def test_function_relation_rendering_only():
    function = SparkRelation.create(
        database="polaris", schema="analytics", identifier="a`b", type="function"
    )
    assert function.render() == "`a``b`"
    table = function.incorporate(type="table")
    assert table.render() == "polaris.analytics.a`b"


def test_core_resolved_alias_is_registration_source_of_truth():
    model = function_model(alias="configured_alias")
    del model["identifier"]
    model["alias"] = "resolved_alias"
    assert SparkFunction.from_model(model).identifier == "resolved_alias"


def test_native_overloads_are_rejected():
    model = function_model()
    model["overloads"] = [{"arguments": []}]
    with pytest.raises(DbtRuntimeError, match="overloads"):
        SparkFunction.from_model(model)


def test_sql_shape_and_quoted_arguments():
    function = SparkFunction.from_model(function_model(volatility="non-deterministic"))
    assert function.sql() == (
        "CREATE OR REPLACE TEMPORARY FUNCTION `double_value`(`value` INT) "
        "RETURNS INT LANGUAGE SQL NOT DETERMINISTIC RETURN value * 2"
    )


def test_registry_idempotence_replacement_new_session_and_cleanup():
    registry = SparkFunctionRegistry()
    function = SparkFunction.from_model(function_model())
    first, second = session_mock(), session_mock()
    registry.build(first, function)
    registry.build(first, function)
    assert first.sql.call_count == 1
    registry.build(first, replace(function, body="value * 3"))
    assert first.sql.call_count == 2
    registry.ensure(second)
    assert "value * 3" in second.sql.call_args.args[0]
    assert registry.cleanup() == []
    assert "DROP TEMPORARY FUNCTION" in first.sql.call_args.args[0]
    assert "DROP TEMPORARY FUNCTION" in second.sql.call_args.args[0]
    first.stop.assert_not_called()
    assert not registry.contains(function.unique_id)


def test_registry_failure_is_not_success():
    registry = SparkFunctionRegistry()
    session = session_mock()
    session.sql.side_effect = RuntimeError("registration failed")
    function = SparkFunction.from_model(function_model())
    with pytest.raises(DbtRuntimeError, match=function.unique_id):
        registry.build(session, function)
    assert not registry.contains(function.unique_id)
    session.sql.side_effect = None
    registry.build(session, function)
    assert registry.contains(function.unique_id)


def test_unowned_function_and_collisions_are_not_replaced():
    registry = SparkFunctionRegistry()
    session = session_mock()
    session.catalog.functionExists.return_value = True
    with pytest.raises(DbtRuntimeError, match="unowned"):
        registry.build(session, SparkFunction.from_model(function_model()))
    session.sql.assert_not_called()
    models = [function_model(), function_model(name="DOUBLE_VALUE")]
    with pytest.raises(DbtRuntimeError, match="collision"):
        registry.validate_names(session, models)
    session.conf.get.return_value = "true"
    registry.validate_names(session, models)


def test_dependencies_require_current_invocation():
    registry = SparkFunctionRegistry()
    with pytest.raises(DbtRuntimeError, match="Select their resources"):
        registry.require(["function.test.missing"])
    function = SparkFunction.from_model(function_model())
    registry.build(session_mock(), function)
    registry.require([function.unique_id])
    registry.cleanup()
    with pytest.raises(DbtRuntimeError):
        registry.require([function.unique_id])


def test_concurrent_registration_once_per_session():
    registry = SparkFunctionRegistry()
    session = session_mock()
    function = SparkFunction.from_model(function_model())
    with ThreadPoolExecutor(4) as workers:
        list(workers.map(lambda _: registry.build(session, function), range(20)))
    assert session.sql.call_count == 1


def test_cursor_prepares_exact_consuming_session(monkeypatch):
    session = session_mock()
    prepare = Mock()
    monkeypatch.setattr("dbt.adapters.spark.session.get_spark_session", lambda _: session)
    cursor = Cursor(prepare_session=prepare)
    cursor.execute("select 1")
    prepare.assert_called_once_with(session)


def test_invalid_types_and_unsupported_runtime_fail_before_mutation():
    registry = SparkFunctionRegistry()
    function = SparkFunction.from_model(function_model())
    session = session_mock()
    session._jsparkSession.sessionState().sqlParser().parseDataType.side_effect = ValueError("bad")
    with pytest.raises(DbtRuntimeError, match="data type"):
        registry.build(session, function)
    session.sql.assert_not_called()
    session.version = "3.5.0"
    with pytest.raises(DbtRuntimeError, match="Spark 4"):
        registry.build(session, function)


def test_cleanup_reports_failure_and_clears_state():
    registry = SparkFunctionRegistry()
    function = SparkFunction.from_model(function_model())
    session = session_mock()
    registry.build(session, function)
    session.sql.side_effect = RuntimeError("cleanup failed")
    errors = registry.cleanup()
    assert len(errors) == 1 and isinstance(errors[0].__cause__, RuntimeError)
    assert not registry.contains(function.unique_id)


def test_pre_model_hook_missing_dependency_is_execution_error():
    adapter = object.__new__(SparkAdapter)
    adapter._functions = SparkFunctionRegistry()
    adapter._behavior = SimpleNamespace(spark_table_only=False)
    model = SimpleNamespace(depends_on=SimpleNamespace(nodes=["function.test.missing"]))

    class Config(dict):
        pass

    config = Config()
    config.model = model
    with pytest.raises(DbtRuntimeError, match="not built"):
        adapter.pre_model_hook(config)


def test_table_only_rejects_views_and_omits_view_discovery():
    import agate

    adapter = object.__new__(SparkAdapter)
    adapter._behavior = SimpleNamespace(spark_table_only=True)
    with pytest.raises(DbtRuntimeError, match="excludes persistent view"):
        adapter.pre_model_hook({"materialized": "view"})
    adapter.execute = Mock(
        return_value=(
            None,
            agate.Table(
                [("analytics", "orders", False)],
                ["namespace", "tableName", "isTemporary"],
            ),
        )
    )
    result = adapter._list_relations_fast(
        SparkRelation.create(database="polaris", schema="analytics")
    )
    assert [relation.identifier for relation in result] == ["orders"]
    assert adapter.execute.call_count == 1
    assert "show tables" in adapter.execute.call_args.args[0]


def test_extension_incompatibility_is_explicit():
    session = session_mock()
    session.conf.get.side_effect = lambda key, default: (
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        if key == "spark.sql.extensions"
        else default
    )
    with pytest.raises(DbtRuntimeError, match="parseRoutineParam"):
        SparkFunction.from_model(function_model()).validate_session(session)


def test_databricks_materialization_and_execution_delegate_to_defaults():
    source = Path("src/dbt/include/spark/macros/materializations/functions.sql").read_text()
    source = source.replace(
        "{% materialization function, adapter='spark', supported_languages=['sql', 'python'] %}",
        "{% macro materialization() %}",
    ).replace("{% endmaterialization %}", "{% endmacro %}")

    class Returned(Exception):
        def __init__(self, value):
            self.value = value

    def return_value(value):
        raise Returned(value)

    adapter = Mock()
    adapter.type.return_value = "databricks"
    default_materialization = Mock(return_value="persistent_function")
    default_execution = Mock(return_value="persistent_ddl")
    template = Environment(extensions=["jinja2.ext.do"]).from_string(source)
    module = template.make_module(
        {
            "adapter": adapter,
            "return": return_value,
            "materialization_function_default": default_materialization,
            "default__function_execute_build_sql": default_execution,
        }
    )
    with pytest.raises(Returned) as result:
        module.materialization()
    assert result.value.value == "persistent_function"
    default_materialization.assert_called_once()
    adapter.validate_spark_function.assert_not_called()
    with pytest.raises(Returned) as result:
        module.spark__function_execute_build_sql("ddl", None, "relation")
    assert result.value.value == "persistent_ddl"
    default_execution.assert_called_once_with("ddl", None, "relation")
