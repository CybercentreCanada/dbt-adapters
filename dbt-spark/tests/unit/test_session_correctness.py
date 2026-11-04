from types import SimpleNamespace
from unittest.mock import Mock

import agate
import pytest

from dbt.adapters.spark import SparkAdapter, SparkColumn, SparkRelation
from dbt.adapters.spark.connections import SparkConnectionMethod
from dbt.adapters.spark.impl import (
    DESCRIBE_TABLE_EXTENDED_MACRO_NAME,
    LIST_RELATIONS_MACRO_NAME,
)
from dbt.adapters.spark.session import Cursor
from dbt_common.exceptions import DbtRuntimeError


@pytest.fixture
def cursor(monkeypatch):
    session = Mock()
    monkeypatch.setattr("dbt.adapters.spark.session.get_spark_session", lambda _: session)
    return Cursor(), session


def dataframe(rows):
    frame = Mock()
    frame.schema.fields = [
        SimpleNamespace(
            name="value",
            dataType=SimpleNamespace(simpleString=lambda: "int"),
            nullable=False,
        )
    ]
    frame.collect.return_value = list(rows)
    frame.toLocalIterator.return_value = iter(rows)
    frame.take.return_value = list(rows[:1])
    return frame


def test_cursor_reuse_invalidates_previous_rows(cursor):
    result, session = cursor
    session.sql.side_effect = [dataframe([(1,)]), dataframe([(2,)])]
    result.execute("select 1")
    assert result.fetchall() == [(1,)]
    result.execute("select 2")
    assert result.fetchall() == [(2,)]


@pytest.mark.parametrize("stage", ["format", "session", "sql"])
def test_failed_execute_invalidates_rows_and_schema(cursor, monkeypatch, stage):
    result, session = cursor
    session.sql.return_value = dataframe([(1,)])
    result.execute("select 1")
    result.fetchall()
    if stage == "session":
        monkeypatch.setattr(
            "dbt.adapters.spark.session.get_spark_session",
            Mock(side_effect=RuntimeError("session unavailable")),
        )
    elif stage == "sql":
        session.sql.side_effect = RuntimeError("execution failed")
    with pytest.raises((RuntimeError, TypeError)):
        result.execute("select %d", "invalid") if stage == "format" else result.execute("bad")
    assert result.description == []
    assert result.fetchone() is None
    assert result.fetchall() is None


@pytest.mark.parametrize("rows", [[], [(1,), (2,), (3,)]])
def test_mixed_fetches_consume_remaining_rows_once(cursor, rows):
    result, session = cursor
    frame = dataframe(rows)
    session.sql.return_value = frame
    result.execute("select rows")
    assert result.fetchone() == (rows[0] if rows else None)
    assert result.fetchall() == rows[1:]
    assert result.fetchone() is None
    assert result.fetchall() == []
    assert result.fetchall() == []
    frame.collect.assert_not_called()
    frame.toLocalIterator.assert_called_once()


def test_fetchall_consumes_rows_and_close_releases_results(cursor):
    result, session = cursor
    session.sql.return_value = dataframe([(1,), (2,)])
    result.execute("select rows")
    assert result.fetchall() == [(1,), (2,)]
    assert result.fetchall() == []
    result.close()
    result.close()
    assert result.description == []
    assert result.fetchone() is None
    assert result.fetchall() is None
    session.stop.assert_not_called()


def test_context_exit_closes_without_suppressing_body_exception(cursor):
    result, session = cursor
    session.sql.return_value = dataframe([(1,)])
    error = ValueError("body failed")
    with pytest.raises(ValueError) as raised:
        with result:
            result.execute("select 1")
            raise error
    assert raised.value is error
    assert result.description == []
    assert result.fetchall() is None


@pytest.fixture
def adapter(monkeypatch):
    instance = object.__new__(SparkAdapter)
    monkeypatch.setattr(instance, "_catalog_is_iceberg_only", lambda _: False)
    return instance


@pytest.mark.parametrize("database", [None, "catalog"])
@pytest.mark.parametrize("is_iceberg,kind", [(True, "table"), (False, "table"), (False, "view")])
def test_catalog_columns_have_exact_relation_identity(
    adapter, monkeypatch, database, is_iceberg, kind
):
    relation = SparkRelation.create(
        database=database,
        schema="analytics",
        identifier="orders",
        type=kind,
        is_iceberg=is_iceberg,
        information="",
    )
    column = SparkColumn(
        table_database=database,
        table_schema="analytics",
        table_name="orders",
        table_type=kind,
        column="id",
        dtype="bigint",
    )
    monkeypatch.setattr(adapter, "get_columns_in_relation", Mock(return_value=[column]))
    rows = list(adapter._get_columns_for_catalog(relation))
    assert len(rows) == 1
    assert {
        key: rows[0][key]
        for key in (
            "table_database",
            "table_schema",
            "table_name",
            "table_type",
            "column_name",
            "column_type",
        )
    } == {
        "table_database": database,
        "table_schema": "analytics",
        "table_name": "orders",
        "table_type": kind,
        "column_name": "id",
        "column_type": "bigint",
    }


def test_catalog_permission_error_propagates(adapter, monkeypatch):
    relation = SparkRelation.create(schema="analytics", identifier="orders", information="")
    monkeypatch.setattr(
        adapter, "get_columns_in_relation", Mock(side_effect=DbtRuntimeError("Permission denied"))
    )
    with pytest.raises(DbtRuntimeError, match="Permission denied"):
        list(adapter._get_columns_for_catalog(relation))


@pytest.mark.parametrize(
    "database,schema,name,expected",
    [
        (None, "analytics", "orders", "analytics.orders"),
        ("catalog", "analytics", "orders", "catalog.analytics.orders"),
        ("cat.name", "a,b", "an`order", "`cat.name`.`a,b`.`an``order`"),
    ],
)
def test_describe_target_uses_relation_rendering(
    adapter, monkeypatch, database, schema, name, expected
):
    relation = SparkRelation.create(database=database, schema=schema).quote(
        database=database == "cat.name", schema=schema == "a,b", identifier=name == "an`order"
    )
    describe = Mock(return_value=[("Type", "VIEW", None)])
    monkeypatch.setattr(adapter, "execute_macro", describe)
    assert adapter._get_relation_information_using_describe((schema, name, False), relation) == (
        schema,
        name,
        "Type: VIEW\n",
    )
    describe.assert_called_once_with(
        DESCRIBE_TABLE_EXTENDED_MACRO_NAME, kwargs={"table_name": expected}
    )


def test_describe_respects_include_policy(adapter, monkeypatch):
    schema = (
        SparkRelation.create(database="omitted", schema="analytics")
        .include(database=False)
        .without_identifier()
    )
    describe = Mock(return_value=[("Type", "VIEW", None)])
    monkeypatch.setattr(adapter, "execute_macro", describe)
    adapter._get_relation_information_using_describe(("analytics", "orders", False), schema)
    assert describe.call_args.kwargs["kwargs"]["table_name"] == "analytics.orders"


def test_relation_escaping_preserves_custom_quote_character():
    relation = SparkRelation.create(identifier='an"order', quote_character='"').quote(
        identifier=True
    )
    assert relation.render() == '"an""order"'


def test_disappeared_relation_does_not_discard_survivors(adapter, monkeypatch):
    schema = SparkRelation.create(schema="analytics")
    rows = agate.Table(
        [("analytics", "gone", False), ("analytics", "report", False)],
        ["namespace", "tableName", "isTemporary"],
    )
    monkeypatch.setattr(
        adapter,
        "execute_macro",
        Mock(
            side_effect=[
                rows,
                DbtRuntimeError("[TABLE_OR_VIEW_NOT_FOUND] gone"),
                [
                    ("Type", "VIEW", None),
                ],
            ]
        ),
    )
    relations = adapter._list_relations_v2_first({"schema_relation": schema}, schema)
    assert [(r.identifier, r.type) for r in relations] == [("report", "view")]


@pytest.mark.parametrize("v2_first", [False, True])
def test_describe_failure_cannot_become_successful_empty_metadata(adapter, monkeypatch, v2_first):
    schema = SparkRelation.create(schema="analytics")
    rows = agate.Table([("analytics", "orders", False)], ["namespace", "name", "temp"])

    def execute_macro(name, **kwargs):
        if name == LIST_RELATIONS_MACRO_NAME:
            raise DbtRuntimeError("SHOW TABLE EXTENDED is not supported for v2 tables")
        if name == DESCRIBE_TABLE_EXTENDED_MACRO_NAME:
            raise DbtRuntimeError("Permission denied")
        return rows

    monkeypatch.setattr(adapter, "execute_macro", execute_macro)
    listing = adapter._list_relations_v2_first if v2_first else adapter._list_relations_v1_first
    with pytest.raises(DbtRuntimeError, match="Permission denied"):
        listing({"schema_relation": schema}, schema)


@pytest.mark.parametrize("message", ["Permission denied", "Invalid identifier", "Runtime failure"])
def test_describe_errors_do_not_trigger_unrelated_fallback(adapter, monkeypatch, message):
    schema = SparkRelation.create(schema="analytics")
    rows = agate.Table([("analytics", "orders", False)], ["namespace", "name", "temp"])
    macro = Mock(side_effect=[rows, DbtRuntimeError(message)])
    monkeypatch.setattr(adapter, "execute_macro", macro)
    with pytest.raises(DbtRuntimeError, match=message):
        adapter._list_relations_v2_first({"schema_relation": schema}, schema)
    assert macro.call_count == 2


def test_empty_describe_is_not_a_fabricated_table(adapter, monkeypatch):
    schema = SparkRelation.create(schema="analytics")
    monkeypatch.setattr(adapter, "execute_macro", Mock(return_value=[]))
    with pytest.raises(DbtRuntimeError, match="DESCRIBE returned no metadata"):
        adapter._get_relation_information_using_describe(("analytics", "orders", False), schema)


@pytest.mark.parametrize("case_sensitive", [False, True])
def test_partition_check_uses_live_session_case_sensitivity(adapter, monkeypatch, case_sensitive):
    adapter.config = SimpleNamespace(
        credentials=SimpleNamespace(
            method=SparkConnectionMethod.SESSION,
            server_side_parameters={"configured": "value"},
        )
    )
    adapter._behavior = SimpleNamespace(
        check_partition_sync=True, check_partition_sync_raises=True
    )
    session = Mock()
    session.conf.get.return_value = str(case_sensitive).lower()
    acquire = Mock(return_value=session)
    monkeypatch.setattr("dbt.adapters.spark.session.get_spark_session", acquire)
    monkeypatch.setattr(
        adapter,
        "execute_macro",
        Mock(
            return_value=agate.Table(
                [
                    ("CREATE TABLE orders (id BIGINT) USING iceberg PARTITIONED BY (id)",),
                ],
                ["createtab_stmt"],
            )
        ),
    )
    relation = SparkRelation.create(schema="analytics", identifier="orders")
    if case_sensitive:
        with pytest.raises(DbtRuntimeError, match="Partition drift detected"):
            adapter.check_partition_sync(relation, "iceberg", "ID")
    else:
        adapter.check_partition_sync(relation, "iceberg", "ID")
    acquire.assert_called_once_with({"configured": "value"})
    session.conf.get.assert_called_once_with("spark.sql.caseSensitive")


@pytest.mark.parametrize("fast_listing", [False, True])
def test_listing_preserves_relation_rendering_policies(adapter, monkeypatch, fast_listing):
    schema = (
        SparkRelation.create(database="omitted", schema="my.schema")
        .quote(database=True, schema=True, identifier=True)
        .include(database=False)
        .without_identifier()
    )
    if fast_listing:
        monkeypatch.setattr(
            adapter,
            "execute",
            Mock(
                side_effect=[
                    (None, [("my.schema", "an`order", False)]),
                    (None, []),
                ]
            ),
        )
        relations = adapter._list_relations_fast(schema)
    else:
        relations = adapter._build_spark_relation_list(
            [("my.schema", "an`order", False)],
            lambda row, _: (row[0], row[1], "Type: MANAGED\n"),
            schema,
        )
    assert len(relations) == 1
    assert relations[0].render() == "`my.schema`.`an``order`"
    assert relations[0].database == "omitted"


@pytest.mark.parametrize("enabled,file_format", [(False, "iceberg"), (True, "parquet")])
def test_partition_check_gates_remain_unchanged(adapter, monkeypatch, enabled, file_format):
    adapter._behavior = SimpleNamespace(check_partition_sync=enabled)
    macro = Mock()
    monkeypatch.setattr(adapter, "execute_macro", macro)
    adapter.check_partition_sync(SparkRelation.create(identifier="orders"), file_format, 42)
    macro.assert_not_called()


@pytest.mark.parametrize(
    "ddl,partition_by",
    [
        ("CREATE TABLE orders (id BIGINT) USING iceberg", 42),
        ("unparseable metadata", None),
    ],
)
def test_invalid_partition_input_fails_even_in_warning_mode(
    adapter, monkeypatch, ddl, partition_by
):
    adapter.config = SimpleNamespace(
        credentials=SimpleNamespace(
            method=SparkConnectionMethod.SESSION,
            server_side_parameters={},
        )
    )
    adapter._behavior = SimpleNamespace(
        check_partition_sync=True, check_partition_sync_raises=False
    )
    session = Mock()
    session.conf.get.return_value = "false"
    monkeypatch.setattr("dbt.adapters.spark.session.get_spark_session", Mock(return_value=session))
    monkeypatch.setattr(
        adapter, "execute_macro", Mock(return_value=agate.Table([(ddl,)], ["ddl"]))
    )
    with pytest.raises(DbtRuntimeError, match="partition"):
        adapter.check_partition_sync(
            SparkRelation.create(identifier="orders"), "iceberg", partition_by
        )
