import pytest
import json
from dataclasses import asdict

from dbt.adapters.spark.impl import SparkAdapter
from dbt.adapters.spark.relation import SparkRelation
from dbt.adapters.spark.relation_configs.write_order import (
    CurrentWriteOrder,
    WriteOrder,
    WriteOrderManager,
    metadata_order,
)
from dbt_common.exceptions import CompilationError


def test_write_order_normalizes_defaults_and_preserves_quoted_identifiers():
    order = WriteOrder.from_config(
        [" customer_id ", "`customer,id` desc", "`a``b`.total ASC NULLS LAST"]
    )
    assert order.sql == (
        "`customer_id` ASC NULLS FIRST, `customer,id` DESC NULLS LAST, "
        "`a``b`.`total` ASC NULLS LAST"
    )
    assert order == WriteOrder.from_config(
        ["customer_id asc nulls first", "`customer,id` DESC NULLS LAST", "`a``b`.total nulls last"]
    )


@pytest.mark.parametrize(
    "value", [None, "id", [None], [1], [""], ["bucket(16, id)"], ["id + 1"], ["id; DROP TABLE t"]]
)
def test_invalid_write_order_is_rejected(value):
    with pytest.raises(ValueError, match="write_order"):
        WriteOrder.from_config(value)


def test_duplicate_columns_are_rejected_using_spark_resolution():
    with pytest.raises(ValueError, match="Duplicate"):
        WriteOrder.from_config(["ID", "`id` desc"]).validate_identifiers(case_sensitive=False)


@pytest.mark.parametrize("materialized", ["view", "ephemeral", "streaming", "snapshot"])
def test_explicit_empty_order_is_rejected_on_unsupported_materializations(materialized):
    adapter = SparkAdapter.__new__(SparkAdapter)
    with pytest.raises(CompilationError, match="orders.*batch table/incremental"):
        adapter.validate_write_order_config(
            {"write_order": [], "materialized": materialized}, "orders"
        )


def test_omission_is_distinct_from_explicit_null():
    adapter = SparkAdapter.__new__(SparkAdapter)
    assert adapter.validate_write_order_config({}) == WriteOrder()
    with pytest.raises(CompilationError, match="write_order"):
        adapter.validate_write_order_config({"write_order": None})


def test_metadata_resolves_field_ids_without_splitting_quoted_column_names():
    metadata = {
        "current-schema-id": 0,
        "schemas": [
            {"schema-id": 0, "fields": [{"id": 1, "name": "customer,id", "type": "long"}]}
        ],
        "default-sort-order-id": 1,
        "sort-orders": [
            {
                "order-id": 1,
                "fields": [
                    {
                        "source-id": 1,
                        "transform": "identity",
                        "direction": "desc",
                        "null-order": "nulls-last",
                    }
                ],
            }
        ],
    }
    assert metadata_order(metadata) == WriteOrder.from_config(["`customer,id` DESC"])
    relation = SparkRelation.create(
        identifier="orders",
        write_order=asdict(
            CurrentWriteOrder(metadata_order(metadata), "authoritative_iceberg_metadata")
        ),
    )
    assert relation.write_order.state == "ordered"
    assert relation.incorporate(identifier="copy").write_order == relation.write_order


@pytest.mark.parametrize("version", ["3.5.6", "4.1.2", "5.0.0"])
def test_unsupported_runtimes_preserve_omission_and_reject_explicit_config(version):
    manager = WriteOrderManager(lambda _: [(version,)])
    assert manager.supported(False) is False
    with pytest.raises(ValueError, match="certified Spark 4.0.x"):
        manager.supported(True)


def test_missing_extensions_is_an_actionable_failure():
    manager = WriteOrderManager(
        lambda sql: [("4.0.3",)] if sql.startswith("SELECT") else [("", "")]
    )
    with pytest.raises(ValueError, match="preconfigured"):
        manager.enabled(None, True, {})


def test_incomplete_describe_is_unknown_not_unordered():
    assert CurrentWriteOrder.from_describe([]).state == "unknown"
    assert CurrentWriteOrder.from_describe([("Provider", "iceberg")]).state == "unknown"


def test_complete_describe_captures_simple_order_but_not_ambiguous_display():
    rows = [
        ("customer_id", "bigint"),
        ("# Detailed Table Information", ""),
        ("Provider", "iceberg"),
        ("Table Properties", "[format-version=2,sort-order=customer_id ASC NULLS LAST]"),
    ]
    assert CurrentWriteOrder.from_describe(rows).order == WriteOrder.from_config(
        ["customer_id ASC NULLS LAST"]
    )
    rows[-1] = ("Table Properties", "[format-version=2,sort-order=customer,id ASC NULLS LAST]")
    assert CurrentWriteOrder.from_describe(rows).state == "unknown"
    rows[-1] = ("Table Properties", "[format-version=2]")
    assert CurrentWriteOrder.from_describe(rows).state == "unordered"


def test_non_iceberg_existing_target_preserves_omission_and_rejects_explicit_order():
    responses = {
        "SELECT version()": [("4.0.3",)],
        "SET spark.sql.extensions": [
            (
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
        ],
        "DESCRIBE TABLE EXTENDED db.orders": [("Provider", "delta", "")],
    }
    manager = WriteOrderManager(responses.__getitem__)
    assert manager.enabled("db.orders", False, {}) is False
    with pytest.raises(ValueError, match="not an Iceberg"):
        manager.enabled("db.orders", True, {})


def test_unsupported_format_preserves_omission_without_requiring_sort_metadata_or_extensions():
    def execute(sql):
        if sql == "SELECT version()":
            return [("4.0.3",)]
        if sql.startswith("DESCRIBE"):
            return [("Provider", "iceberg", "")]
        if sql.startswith("SELECT file"):
            return [("file:///metadata.json",)]
        if sql.startswith("SELECT value"):
            return [(json.dumps({"format-version": 1}),)]
        if sql.startswith(("CREATE TEMPORARY VIEW", "DROP VIEW")):
            return []
        raise AssertionError(f"Unexpected SQL on unsupported format: {sql}")

    manager = WriteOrderManager(execute)
    assert manager.enabled("db.orders", False, {}) is False
    with pytest.raises(ValueError, match="format v2/v3"):
        manager.enabled("db.orders", True, {})
