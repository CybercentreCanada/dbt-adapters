"""Opt-in local Spark tests for the streaming materialization."""

import os
import re
import subprocess
import time
import uuid
from pathlib import Path

import pytest
from jinja2 import Environment
from pyspark.sql import SparkSession


pytestmark = pytest.mark.skipif(
    os.environ.get("DBT_RUN_LOCAL_SPARK_TESTS") != "1",
    reason="Run with hatch run local-spark-tests:test",
)

STREAMING_TEMPLATE = (
    Path(__file__).parents[2] / "src/dbt/include/spark/macros/materializations/streaming.sql"
)
ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0"


class _Config:
    def __init__(self, values):
        self.values = values

    def get(self, key, default=None):
        return self.values.get(key, default)


class _BehaviorFlag:
    no_warn = False


class _Dbt:
    def __init__(self, ref):
        self.ref = ref


def _java_major_version() -> int:
    java_version = subprocess.run(
        ["java", "--version"], capture_output=True, check=True, text=True
    ).stdout
    match = re.search(r"(?:openjdk|java) (\d+)", java_version)
    if match is None:
        raise RuntimeError(f"Unable to determine Java version: {java_version}")
    return int(match.group(1))


def _jinja_source() -> str:
    source = STREAMING_TEMPLATE.read_text().replace(
        "{% materialization streaming, adapter='spark', supported_languages=['sql', 'python'] %}",
        "{% macro streaming() %}",
    )
    return source.replace("{% endmaterialization %}", "{% endmacro %}")


def _render_streaming_model(
    target_name: str, checkpoint_basedir: Path, partition_by: list[str] | None = None
) -> str:
    template = Environment(extensions=["jinja2.ext.do"]).from_string(_jinja_source())
    template.globals.update(
        {
            "config": _Config(
                {
                    "file_format": "iceberg",
                    "checkpoint_basedir": str(checkpoint_basedir),
                    "partition_by": partition_by,
                    "trigger": "1 second",
                    "stream_options": None,
                    "read_stream_options": {},
                    "write_stream_options": {},
                }
            ),
            "adapter": type(
                "Adapter",
                (),
                {"behavior": type("Behavior", (), {"await_termination": _BehaviorFlag()})()},
            )(),
            "env_var": lambda _, default: default,
            "exceptions": type(
                "Exceptions",
                (),
                {
                    "raise_compiler_error": staticmethod(
                        lambda message: (_ for _ in ()).throw(ValueError(message))
                    )
                },
            )(),
            "location_clause": lambda: "",
            "python__partitionedBy_clause": lambda: (
                '  writer = writer.partitionedBy(days("timestamp"))'
                if partition_by == ["days(timestamp)"]
                else ""
            ),
            "python__location_clause": lambda: "",
            "python__tblproperties_clause": lambda: "",
            "return": lambda value: value,
            "spark__escape_single_quotes": lambda value: value.replace("'", "\\'"),
            "spark__stream_options_clause": lambda: "",
        }
    )
    compiled_code = """def model(dbt, spark):
    return spark.readStream.format(\"rate\").option(\"rowsPerSecond\", 10).load()
"""
    return template.module.py_stream_table(compiled_code, target_name, False)


@pytest.mark.parametrize("partition_by", [None, ["days(timestamp)"]])
def test_streaming_materialization_live_template_renders_without_location_root(
    tmp_path, partition_by
):
    rendered = _render_streaming_model(
        "test_catalog.default.streaming_test", tmp_path, partition_by
    )

    compile(rendered, "streaming_model.py", "exec")

    assert 'writer = writer.option("path",' not in rendered
    assert '.split("\'")' not in rendered
    if partition_by:
        assert '        writer = writer.partitionedBy(days("timestamp"))' in rendered


def _wait_for_rows(spark: SparkSession, target_name: str, timeout_seconds: int = 30) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        try:
            if spark.table(target_name).limit(1).count() == 1:
                return
        except Exception:
            pass
        time.sleep(0.2)
    pytest.fail(f"No rows were committed to {target_name} within {timeout_seconds} seconds")


def _table_partition_spec(spark: SparkSession, target_name: str) -> list[str]:
    ddl = spark.sql(f"SHOW CREATE TABLE {target_name}").first()[0]
    match = re.search(
        r"PARTITIONED\s+BY\s*\((.+?)\)\s*(?:TBLPROPERTIES|LOCATION|OPTIONS|COMMENT|$)",
        ddl,
        re.IGNORECASE | re.DOTALL,
    )
    if match is None:
        return []

    partitions = []
    depth = 0
    current = []
    for character in match.group(1).strip():
        if character == "(":
            depth += 1
        elif character == ")":
            depth -= 1
        if character == "," and depth == 0:
            partitions.append("".join(current))
            current = []
        else:
            current.append(character)
    if current:
        partitions.append("".join(current))

    return [
        re.sub(r"\s+", " ", partition.replace("`", "")).strip().lower() for partition in partitions
    ]


@pytest.fixture
def spark(tmp_path):
    java_major_version = _java_major_version()
    if java_major_version not in {11, 17}:
        pytest.skip("Local Spark tests require Java 11 or 17; " f"found Java {java_major_version}")

    warehouse = tmp_path / "warehouse"
    session = (
        SparkSession.builder.master("local[*]")
        .appName("dbt-spark-streaming-materialization-tests")
        .config("spark.driver.memory", "1g")
        .config("spark.driver.cores", "1")
        .config("spark.driver.maxResultSize", "1G")
        .config("spark.cores.max", "4")
        .config("spark.executor.cores", "4")
        .config("spark.executor.memory", "1g")
        .config("spark.jars.packages", ICEBERG_PACKAGE)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.test_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.test_catalog.type", "hadoop")
        .config("spark.sql.catalog.test_catalog.warehouse", warehouse.as_uri())
        .config("spark.sql.defaultCatalog", "test_catalog")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    yield session
    for query in session.streams.active:
        query.stop()
        query.awaitTermination(10)
    session.stop()


def test_streaming_materialization_starts_and_reuses_local_iceberg_stream(spark, tmp_path):
    target_name = f"test_catalog.default.streaming_{uuid.uuid4().hex}"
    rendered = _render_streaming_model(target_name, tmp_path / "checkpoints")
    namespace = {"dbtObj": _Dbt, "spark": spark}

    exec(compile(rendered, "streaming_model.py", "exec"), namespace)

    active_query = next(query for query in spark.streams.active if query.name == target_name)
    _wait_for_rows(spark, target_name)

    exec(compile(rendered, "streaming_model.py", "exec"), namespace)

    reused_query = next(query for query in spark.streams.active if query.name == target_name)
    assert reused_query.id == active_query.id

    active_query.stop()
    assert active_query.awaitTermination(10)


def test_streaming_materialization_creates_partitioned_local_iceberg_stream(spark, tmp_path):
    target_name = f"test_catalog.default.streaming_{uuid.uuid4().hex}"
    rendered = _render_streaming_model(
        target_name, tmp_path / "checkpoints", partition_by=["days(timestamp)"]
    )
    namespace = {"dbtObj": _Dbt, "spark": spark}

    assert 'writer = writer.partitionedBy(days("timestamp"))' in rendered

    exec(compile(rendered, "streaming_partitioned_model.py", "exec"), namespace)
    active_query = next(query for query in spark.streams.active if query.name == target_name)
    try:
        _wait_for_rows(spark, target_name)
        assert _table_partition_spec(spark, target_name) == ["days(timestamp)"]
    finally:
        active_query.stop()
        active_query.awaitTermination(10)
