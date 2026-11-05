"""Opt-in policy and lifecycle tests on Spark 4.0 / Iceberg formats v2 and v3."""

import os
import json
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest
import agate
from jinja2 import Environment
from pyspark.sql import SparkSession

from dbt.adapters.spark.impl import SparkAdapter
from dbt.adapters.spark.relation import SparkRelation
from dbt.adapters.contracts.connection import AdapterResponse
from dbt.adapters.spark.relation_configs.write_order import WriteOrder, WriteOrderManager
from dbt.cli.main import dbtRunner
from tests.unit.test_write_order_parse import write_project


pytestmark = pytest.mark.skipif(
    os.environ.get("DBT_RUN_WRITE_ORDER_TESTS") != "1",
    reason="Run with hatch run write-order-tests:test",
)


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    warehouse = tmp_path_factory.mktemp("write-order-warehouse")
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("dbt-spark-write-order-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.jars.repositories", "https://repo.maven.apache.org/maven2")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.test_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.test_catalog.type", "hadoop")
        .config("spark.sql.catalog.test_catalog.warehouse", warehouse.as_uri())
        .config("spark.sql.defaultCatalog", "test_catalog")
    )
    if os.environ.get("DBT_ICEBERG_JAR"):
        builder = builder.config("spark.jars", os.environ["DBT_ICEBERG_JAR"])
    else:
        builder = builder.config(
            "spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0"
        )
    session = builder.getOrCreate()
    if not session.version.startswith("4.0."):
        session.stop()
        pytest.fail("Write-order certification requires Spark 4.0.x")
    session.sql("CREATE NAMESPACE IF NOT EXISTS test_catalog.default").collect()
    yield session
    session.stop()


class _LiveAdapter(SparkAdapter):
    def __init__(self, spark):
        self.spark = spark

    def _write_order_execute(self, sql):
        return self.spark.sql(sql).collect()

    def execute(self, sql, auto_begin=False, fetch=False, limit=None):
        frame = self.spark.sql(sql)
        rows = frame.collect()
        return AdapterResponse(_message="OK"), agate.Table(
            [tuple(row) for row in rows], frame.columns
        )

    @property
    def behavior(self):
        return SimpleNamespace(require_location_root=False)


class _Config:
    def __init__(self, values):
        self.values = values

    def get(self, key, default=None, **kwargs):
        return self.values.get(key, default)


def _render_python(adapter, relation, config, exists, query):
    source = Path("src/dbt/include/spark/macros/materializations/table.sql").read_text()
    source = source.replace(
        "{% materialization table, adapter = 'spark', supported_languages=['sql', 'python'] %}",
        "{% macro table() %}",
    ).replace("{% endmaterialization %}", "{% endmacro %}")
    template = Environment(extensions=["jinja2.ext.do"]).from_string(
        Path("src/dbt/include/spark/macros/adapters.sql").read_text() + source
    )
    template.globals.update(
        config=_Config(config),
        model=SimpleNamespace(config=config, alias=relation.identifier),
        adapter=adapter,
        validation=SimpleNamespace(any={}),
        spark__filtered_tblproperties=lambda value: value,
        spark__escape_single_quotes=str,
    )
    # These globals are only consumed for absent optional configuration.
    template.globals["validation"].any = type(
        "Validation", (), {"__class_getitem__": classmethod(lambda cls, value: value)}
    )
    template.globals["return"] = lambda value: value
    compiled = f"def model(dbt, spark):\n    return spark.sql({query!r})\n"
    return template.module.py_write_table(compiled, relation, False, True, exists)


QUERY = "SELECT * FROM VALUES (1, 20), (2, NULL), (1, 10) AS t(customer_id, total)"


@pytest.mark.parametrize("format_version", [2, 3])
@pytest.mark.parametrize("language", ["sql", "python"])
def test_ordered_creation_changes_clearing_and_atomic_replacement(spark, format_version, language):
    adapter = _LiveAdapter(spark)
    relation = SparkRelation.create(
        database="test_catalog", schema="default", identifier="orders_" + uuid.uuid4().hex
    )
    target = adapter._write_order_target(relation)
    manager = WriteOrderManager(adapter._write_order_execute)
    config = {
        "materialized": "table",
        "file_format": "iceberg",
        "tblproperties": {"format-version": str(format_version)},
        "write_order": ["customer_id ASC NULLS LAST", "total DESC"],
    }

    def materialize(exists, query=QUERY):
        if language == "sql":
            adapter.materialize_write_order_sql(
                relation, query, "USING iceberg", "", config, exists
            )
        else:
            rendered = _render_python(adapter, relation, config, exists, query)
            exec(
                compile(rendered, "write_order_model.py", "exec"),
                {
                    "spark": spark,
                    "dbtObj": lambda loader: SimpleNamespace(),
                },
            )

    try:
        assert adapter.write_order_enabled(None, config)
        materialize(False)
        assert manager.current(target).order == WriteOrder.from_config(config["write_order"])
        assert manager.metadata(target)["format-version"] == format_version
        assert spark.table(target).count() == 3
        # Metadata commits provide evidence that the policy preceded the first rows.
        log = spark.sql(
            f"SELECT file FROM {target}.metadata_log_entries ORDER BY timestamp"
        ).collect()
        assert len(log) >= 3
        for row in log:
            committed = json.loads(
                spark.read.option("multiLine", "true").json(row[0]).toJSON().first()
            )
            if any(
                int(snapshot["summary"].get("added-records", 0)) > 0
                for snapshot in committed.get("snapshots", [])
            ):
                assert committed["default-sort-order-id"] != 0

        config["write_order"] = ["total ASC NULLS LAST"]
        adapter.sync_write_order(relation, config)
        assert manager.current(target).order == WriteOrder.from_config(config["write_order"])
        before = spark.sql(f"SELECT count(*) FROM {target}.metadata_log_entries").first()[0]
        config["write_order"] = [" total asc nulls last "]
        adapter.sync_write_order(relation, config)
        after = spark.sql(f"SELECT count(*) FROM {target}.metadata_log_entries").first()[0]
        assert after == before
        assert spark.table(target).count() == 3

        config.pop("write_order")
        adapter.sync_write_order(relation, config)
        assert manager.current(target).state == "unordered"
        config["write_order"] = ["customer_id DESC"]
        materialize(True)
        assert manager.current(target).order == WriteOrder.from_config(config["write_order"])
        config["write_order"] = []
        materialize(True)
        assert manager.current(target).state == "unordered"
        assert spark.table(target).count() == 3
        failed_query = (
            "SELECT CASE WHEN customer_id > 0 THEN "
            "CAST(raise_error('injected replacement write failure') AS INT) "
            f"ELSE customer_id END AS customer_id, total FROM ({QUERY}) AS failing"
        )
        with pytest.raises(Exception, match="injected replacement write failure"):
            materialize(True, failed_query)
        assert spark.table(target).count() == 3
        assert manager.current(target).state == "unordered"
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {target}").collect()


@pytest.mark.parametrize("format_version", [2, 3])
def test_invalid_order_preserves_existing_target_and_does_not_publish_new_target(
    spark, format_version
):
    adapter = _LiveAdapter(spark)
    relation = SparkRelation.create(
        database="test_catalog", schema="default", identifier="invalid_" + uuid.uuid4().hex
    )
    target = adapter._write_order_target(relation)
    config = {
        "materialized": "table",
        "file_format": "iceberg",
        "tblproperties": {"format-version": str(format_version)},
        "write_order": ["missing_column"],
    }
    with pytest.raises(Exception, match="missing_column"):
        adapter.materialize_write_order_sql(relation, QUERY, "USING iceberg", "", config, False)
    assert not spark.catalog.tableExists(target)
    config["write_order"] = ["customer_id"]
    adapter.materialize_write_order_sql(relation, QUERY, "USING iceberg", "", config, False)
    config["write_order"] = ["missing_column"]
    try:
        with pytest.raises(Exception, match="missing_column"):
            adapter.materialize_write_order_sql(relation, QUERY, "USING iceberg", "", config, True)
        assert spark.table(target).count() == 3
    finally:
        spark.sql(f"DROP TABLE {target}").collect()


@pytest.mark.parametrize("format_version", [2, 3])
def test_quoted_identifiers_round_trip_through_authoritative_metadata(spark, format_version):
    adapter = _LiveAdapter(spark)
    relation = SparkRelation.create(
        database="test_catalog", schema="default", identifier="quoted." + uuid.uuid4().hex
    )
    target = adapter._write_order_target(relation)
    config = {
        "materialized": "table",
        "file_format": "iceberg",
        "tblproperties": {"format-version": str(format_version)},
        "write_order": [
            "`customer,id` DESC",
            "`a``b` ASC NULLS LAST",
            "`customer id`",
            "`a.b` DESC",
            "container.nested DESC",
        ],
    }
    try:
        adapter.materialize_write_order_sql(
            relation,
            "SELECT 1 AS `customer,id`, 2 AS `a``b`, 3 AS `customer id`, "
            "4 AS `a.b`, named_struct('nested', 5) AS container",
            "USING iceberg",
            "",
            config,
            False,
        )
        current = adapter.get_write_order_relation(relation)
        assert current.write_order.order == WriteOrder.from_config(config["write_order"])
        adapter.sync_write_order(relation, config)
        assert spark.table(target).count() == 1
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {target}").collect()


@pytest.mark.parametrize("exists", [False, True])
def test_failed_write_cleanup_and_atomic_replacement_contract(spark, exists):
    target = "test_catalog.default.failure_" + uuid.uuid4().hex
    manager = WriteOrderManager(lambda sql: spark.sql(sql).collect())

    def create_empty(name, owner):
        spark.sql(
            f"CREATE TABLE {name} USING iceberg "
            f"TBLPROPERTIES ('dbt.write-order-owner'='{owner}') AS SELECT 1 AS id WHERE false"
        ).collect()

    def failed_write(replace):
        assert replace == exists
        if not exists:
            assert manager.current(target).order == WriteOrder.from_config(["id DESC"])
        raise RuntimeError("injected data write failure")

    if exists:
        spark.sql(f"CREATE TABLE {target} USING iceberg AS SELECT 42 AS id").collect()
    try:
        with pytest.raises(RuntimeError, match="injected data write failure"):
            manager.materialize(
                target, WriteOrder.from_config(["id DESC"]), exists, create_empty, failed_write
            )
        assert spark.catalog.tableExists(target) == exists
        if exists:
            assert spark.table(target).first()[0] == 42
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {target}").collect()


def test_post_replacement_alter_failure_leaves_replacement_and_reports_failure(spark):
    target = "test_catalog.default.post_alter_" + uuid.uuid4().hex
    spark.sql(f"CREATE TABLE {target} USING iceberg AS SELECT 42 AS id").collect()

    def execute(sql):
        if sql.startswith(f"ALTER TABLE {target} WRITE"):
            raise RuntimeError("injected ALTER failure")
        return spark.sql(sql).collect()

    manager = WriteOrderManager(execute)

    def create_empty(name, owner):
        spark.sql(
            f"CREATE TABLE {name} USING iceberg "
            f"TBLPROPERTIES ('dbt.write-order-owner'='{owner}') AS SELECT 1 AS id WHERE false"
        ).collect()

    def replace(_):
        spark.sql(f"CREATE OR REPLACE TABLE {target} USING iceberg AS SELECT 99 AS id").collect()

    try:
        with pytest.raises(RuntimeError, match="Replacement.*succeeded.*synchronization failed"):
            manager.materialize(
                target, WriteOrder.from_config(["id DESC"]), True, create_empty, replace
            )
        assert spark.table(target).first()[0] == 99
    finally:
        spark.sql(f"DROP TABLE {target}").collect()


@pytest.mark.parametrize("failure", ["alter", "metadata", "cleanup", "ownership"])
def test_new_target_failures_cleanup_only_owned_tables_and_surface_errors(spark, failure):
    target = "test_catalog.default.cleanup_" + uuid.uuid4().hex

    def execute(sql):
        if failure == "alter" and sql.startswith(f"ALTER TABLE {target} WRITE"):
            raise RuntimeError("injected initial ALTER failure")
        if failure == "metadata" and sql.startswith(f"SELECT file FROM {target}."):
            raise RuntimeError("injected metadata failure")
        if failure == "cleanup" and sql == f"DROP TABLE {target}":
            raise RuntimeError("injected cleanup failure")
        return spark.sql(sql).collect()

    manager = WriteOrderManager(execute)

    def create_empty(name, owner):
        spark.sql(
            f"CREATE TABLE {name} USING iceberg "
            f"TBLPROPERTIES ('dbt.write-order-owner'='{owner}') AS SELECT 1 AS id WHERE false"
        ).collect()

    def write(_):
        if failure == "ownership":
            spark.sql(f"DROP TABLE {target}").collect()
            spark.sql(f"CREATE TABLE {target} USING iceberg AS SELECT 99 AS id").collect()
        raise RuntimeError("injected write failure")

    try:
        with pytest.raises(RuntimeError, match="injected") as error:
            manager.materialize(target, WriteOrder.from_config(["id"]), False, create_empty, write)
        if failure in ("cleanup", "ownership"):
            assert "injected write failure" in str(error.value)
            assert "cleanup failed" in str(error.value)
            assert spark.catalog.tableExists(target)
            if failure == "ownership":
                assert spark.table(target).first()[0] == 99
        else:
            assert not spark.catalog.tableExists(target)
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {target}").collect()


@pytest.mark.parametrize("format_version", [2, 3])
@pytest.mark.parametrize("language", ["sql", "python"])
@pytest.mark.parametrize("materialized", ["table", "incremental"])
def test_dbt_run_materialization_lifecycle(
    spark, tmp_path, format_version, language, materialized
):
    alias = "dbt_orders_" + uuid.uuid4().hex
    target = "test_catalog.default." + alias
    manager = WriteOrderManager(lambda sql: spark.sql(sql).collect())
    extra = {"alias": alias, "tblproperties": {"format-version": str(format_version)}}

    def run(order, full_refresh=False):
        write_project(tmp_path, materialized, language, order, extra)
        args = [
            "run",
            "--no-partial-parse",
            "--project-dir",
            str(tmp_path),
            "--profiles-dir",
            str(tmp_path),
            "--log-path",
            str(tmp_path / "logs"),
        ]
        if full_refresh:
            args.append("--full-refresh")
        result = dbtRunner().invoke(args)
        assert result.success, str(result.exception) or str(result.result)

    try:
        run(["customer_id DESC"])
        assert manager.current(target).order == WriteOrder.from_config(["customer_id DESC"])
        run(["total ASC NULLS LAST"])
        assert manager.current(target).order == WriteOrder.from_config(["total ASC NULLS LAST"])
        run(None)
        assert manager.current(target).state == "unordered"
        run(["customer_id ASC"], True)
        assert manager.current(target).order == WriteOrder.from_config(["customer_id"])
        run([], True)
        assert manager.current(target).state == "unordered"
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {target}").collect()


@pytest.mark.parametrize("strategy", ["merge", "insert_overwrite", "microbatch"])
@pytest.mark.parametrize("format_version", [2, 3])
@pytest.mark.parametrize("language", ["sql", "python"])
def test_dbt_incremental_strategies_sync_before_writing(
    spark, tmp_path, strategy, format_version, language
):
    alias = "strategy_" + uuid.uuid4().hex
    target = "test_catalog.default." + alias
    extra = {
        "alias": alias,
        "tblproperties": {"format-version": str(format_version)},
        "incremental_strategy": strategy,
        "partition_by": ["days(event_time)"],
        "unique_key": "customer_id",
    }
    if strategy == "microbatch":
        extra.update(
            event_time="event_time",
            batch_size="day",
            begin="2026-11-01",
            lookback=0,
            concurrent_batches=True,
        )
    query = "SELECT 1 AS customer_id, 2 AS total, " "TIMESTAMP '2026-11-02 12:00:00' AS event_time"

    def run(order, expect_success=True):
        write_project(tmp_path, "incremental", language, order, extra, query)
        args = [
            "run",
            "--no-partial-parse",
            "--project-dir",
            str(tmp_path),
            "--profiles-dir",
            str(tmp_path),
            "--log-path",
            str(tmp_path / "logs"),
        ]
        if strategy == "microbatch":
            args.extend(["--event-time-start", "2026-11-01", "--event-time-end", "2026-11-04"])
        result = dbtRunner().invoke(args)
        assert result.success == expect_success, result.exception or result.result

    manager = WriteOrderManager(lambda sql: spark.sql(sql).collect())
    try:
        run(["customer_id DESC"])
        run(["total DESC"])
        assert manager.current(target).order == WriteOrder.from_config(["total DESC"])
        run(None)
        assert manager.current(target).state == "unordered"
        if strategy == "microbatch":
            before = manager.metadata(target)
            run(["missing"], False)
            assert manager.metadata(target) == before
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {target}").collect()


@pytest.mark.parametrize("format_version", [2, 3])
@pytest.mark.parametrize("language", ["sql", "python"])
@pytest.mark.parametrize("explicit", [False, True])
def test_new_target_omitted_or_empty_policy_is_unordered(
    spark, format_version, language, explicit
):
    adapter = _LiveAdapter(spark)
    relation = SparkRelation.create(
        database="test_catalog", schema="default", identifier="unordered_" + uuid.uuid4().hex
    )
    target = adapter._write_order_target(relation)
    config = {
        "materialized": "table",
        "file_format": "iceberg",
        "tblproperties": {"format-version": str(format_version)},
    }
    if explicit:
        config["write_order"] = []
    try:
        if language == "sql":
            adapter.materialize_write_order_sql(
                relation, QUERY, "USING iceberg", "", config, False
            )
        else:
            code = _render_python(adapter, relation, config, False, QUERY)
            exec(
                compile(code, "unordered.py", "exec"),
                {
                    "spark": spark,
                    "dbtObj": lambda loader: SimpleNamespace(),
                },
            )
        assert WriteOrderManager(adapter._write_order_execute).current(target).state == "unordered"
        assert spark.table(target).count() == 3
        if format_version == 3:
            config.pop("tblproperties")
            if language == "sql":
                adapter.materialize_write_order_sql(
                    relation, QUERY, "USING iceberg", "", config, True
                )
            else:
                code = _render_python(adapter, relation, config, True, QUERY)
                exec(
                    compile(code, "replace_unordered.py", "exec"),
                    {
                        "spark": spark,
                        "dbtObj": lambda loader: SimpleNamespace(),
                    },
                )
            assert (
                WriteOrderManager(adapter._write_order_execute).metadata(target)["format-version"]
                == 3
            )
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {target}").collect()


@pytest.mark.parametrize("format_version", [2, 3])
@pytest.mark.parametrize("language", ["sql", "python"])
@pytest.mark.parametrize("schema_policy", ["append_new_columns", "sync_all_columns"])
def test_order_can_reference_a_new_column_with_schema_evolution(
    spark, tmp_path, format_version, language, schema_policy
):
    alias = "evolution_" + uuid.uuid4().hex
    target = "test_catalog.default." + alias
    extra = {
        "alias": alias,
        "tblproperties": {"format-version": str(format_version)},
        "on_schema_change": schema_policy,
    }

    def run(order, query):
        write_project(tmp_path, "incremental", language, order, extra, query)
        return dbtRunner().invoke(
            [
                "run",
                "--no-partial-parse",
                "--project-dir",
                str(tmp_path),
                "--profiles-dir",
                str(tmp_path),
                "--log-path",
                str(tmp_path / "logs"),
            ]
        )

    manager = WriteOrderManager(lambda sql: spark.sql(sql).collect())
    try:
        result = run(["customer_id"], "SELECT 1 AS customer_id, 2 AS total")
        assert result.success, result.exception or result.result
        if schema_policy == "sync_all_columns":
            original = manager.metadata(target)
            result = run(["new_sort DESC"], "SELECT 2 AS total, 3 AS new_sort")
            assert not result.success
            assert "does not support dropping columns" in result.result.results[0].message
            remaining = manager.metadata(target)
            for key in ("schemas", "snapshots", "current-snapshot-id", "default-sort-order-id"):
                assert remaining[key] == original[key]
        evolved_query = "SELECT 1 AS customer_id, 2 AS total, 3 AS new_sort"
        result = run(["new_sort DESC"], evolved_query)
        assert result.success, result.exception or result.result
        assert manager.current(target).order == WriteOrder.from_config(["new_sort DESC"])
        before = manager.metadata(target)
        result = run(["missing"], evolved_query + ", 4 AS pending")
        assert not result.success
        assert "pending" not in spark.table(target).columns
        assert manager.metadata(target) == before
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {target}").collect()
