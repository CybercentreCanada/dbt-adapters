import os
import sys
from dataclasses import replace
from uuid import uuid4

import pytest

from dbt.adapters.spark.functions import SparkFunction, SparkFunctionRegistry
from dbt_common.exceptions import DbtRuntimeError

pytestmark = pytest.mark.skipif(
    os.getenv("DBT_RUN_SPARK_FUNCTION_TESTS") != "1",
    reason="Set DBT_RUN_SPARK_FUNCTION_TESTS=1 to run Spark 4.1.3 UDF tests",
)


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("dbt-spark-function-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    assert session.version == "4.1.3"
    yield session
    session.stop()


@pytest.fixture
def registry():
    result = SparkFunctionRegistry()
    yield result
    assert result.cleanup() == []


def function(language, body, arguments=(("value", "INT"),), return_type="INT"):
    name = f"spec_{uuid4().hex}"
    return SparkFunction(
        unique_id=f"function.test.{name}",
        identifier=name,
        language=language,
        arguments=arguments,
        return_type=return_type,
        body=body,
        dependencies=(),
        entry_point="implementation",
        runtime_version=f"{sys.version_info.major}.{sys.version_info.minor}",
    )


@pytest.mark.parametrize("language,body", [("sql", "value * 2"), ("python", "return value * 2")])
def test_registration_replay_replacement_and_isolation(spark, registry, language, body):
    definition = function(language, body)
    registry.build(spark, definition)
    assert spark.sql(f"select `{definition.identifier}`(3)").first()[0] == 6
    fresh = spark.newSession()
    assert not fresh.catalog.functionExists(definition.identifier)
    registry.ensure(fresh)
    assert fresh.sql(f"select `{definition.identifier}`(3)").first()[0] == 6
    updated = replace(definition, body=body.replace("* 2", "* 3"))
    registry.build(spark, updated)
    registry.ensure(fresh)
    assert fresh.sql(f"select `{definition.identifier}`(3)").first()[0] == 9


def test_python_multiple_arguments_nulls_and_distributed_execution(spark, registry):
    definition = function(
        "python",
        "if value is None:\n    return None\nreturn value * multiplier",
        arguments=(("value", "BIGINT"), ("multiplier", "INT")),
        return_type="BIGINT",
    )
    registry.build(spark, definition)
    rows = (
        spark.range(10, numPartitions=2)
        .selectExpr(f"`{definition.identifier}`(id, 3) as result")
        .collect()
    )
    assert [row.result for row in rows] == [i * 3 for i in range(10)]
    assert spark.sql(f"select `{definition.identifier}`(NULL, 3)").first()[0] is None


def test_zero_arguments_and_sql_query_body(spark, registry):
    definition = function("sql", "SELECT CAST(42 AS INT)", arguments=())
    registry.build(spark, definition)
    assert spark.sql(f"select `{definition.identifier}`()").first()[0] == 42


def test_unowned_builtin_and_signature_change(spark, registry):
    with pytest.raises(DbtRuntimeError, match="unowned"):
        registry.build(
            spark,
            replace(function("sql", "value * 2"), unique_id="function.test.abs", identifier="abs"),
        )
    definition = function("sql", "value * 2")
    registry.build(spark, definition)
    registry.build(spark, replace(definition, arguments=(("value", "BIGINT"),)))
    assert spark.sql(f"select `{definition.identifier}`(3)").first()[0] == 6


def test_dependency_replay_updates_compiled_sql_function(spark, registry):
    parent = function("sql", "value * 2")
    child = replace(
        function("sql", f"`{parent.identifier}`(value) + 1"),
        dependencies=(parent.unique_id,),
    )
    registry.build(spark, parent)
    registry.build(spark, child)
    assert spark.sql(f"select `{child.identifier}`(3)").first()[0] == 7
    registry.build(spark, replace(parent, body="value * 3"))
    registry.ensure(spark)
    assert spark.sql(f"select `{child.identifier}`(3)").first()[0] == 10
    fresh = spark.newSession()
    registry.ensure(fresh)
    assert fresh.sql(f"select `{child.identifier}`(3)").first()[0] == 10


def test_python_worker_error_is_not_registration_success_fallback(spark, registry):
    from pyspark.errors import PythonException

    definition = function("python", "raise RuntimeError('row evaluation failed')")
    registry.build(spark, definition)
    with pytest.raises(PythonException, match="row evaluation failed"):
        spark.sql(f"select `{definition.identifier}`(3)").collect()
