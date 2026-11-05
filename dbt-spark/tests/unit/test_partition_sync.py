"""Unit tests for partition drift detection logic."""

import agate
import pytest

from dbt_common.exceptions import DbtRuntimeError
from dbt.adapters.spark.relation_configs.partitions import (
    PartitionConfig,
    PartitionProcessor,
    _parse_partition_spec,
    _normalize_model_partitions,
)


class TestParsePartitionSpec:
    def test_simple_columns(self):
        ddl = """
        CREATE TABLE spark_catalog.my_schema.my_table (
            id BIGINT,
            name STRING,
            dt DATE
        )
        USING iceberg
        PARTITIONED BY (dt)
        TBLPROPERTIES ('format-version' = '2')
        """
        assert _parse_partition_spec(ddl) == ["dt"]

    def test_multiple_columns(self):
        ddl = """
        CREATE TABLE spark_catalog.my_schema.my_table (
            id BIGINT,
            category STRING,
            dt DATE
        )
        USING iceberg
        PARTITIONED BY (category, dt)
        LOCATION 's3://bucket/path'
        """
        assert _parse_partition_spec(ddl) == ["category", "dt"]

    def test_transform_expressions(self):
        ddl = """
        CREATE TABLE spark_catalog.my_schema.my_table (
            id BIGINT,
            ts TIMESTAMP,
            category STRING
        )
        USING iceberg
        PARTITIONED BY (days(ts), bucket(16, id))
        TBLPROPERTIES ('format-version' = '2')
        """
        assert _parse_partition_spec(ddl) == ["days(ts)", "bucket(16, id)"]

    def test_mixed_transforms_and_columns(self):
        ddl = """
        CREATE TABLE catalog.schema.table (
            id BIGINT,
            ts TIMESTAMP,
            region STRING
        )
        USING iceberg
        PARTITIONED BY (region, months(ts))
        TBLPROPERTIES ('write.format.default' = 'parquet')
        """
        assert _parse_partition_spec(ddl) == ["region", "months(ts)"]

    def test_no_partition_clause(self):
        ddl = """
        CREATE TABLE spark_catalog.my_schema.my_table (
            id BIGINT
        )
        USING iceberg
        TBLPROPERTIES ('format-version' = '2')
        """
        assert _parse_partition_spec(ddl) == []

    def test_backtick_quoted_columns(self):
        ddl = """
        CREATE TABLE catalog.schema.table (
            `my column` STRING
        )
        USING iceberg
        PARTITIONED BY (`my column`)
        TBLPROPERTIES ('format-version' = '2')
        """
        assert _parse_partition_spec(ddl) == ["`my column`"]

    def test_case_insensitive(self):
        ddl = """
        CREATE TABLE t (id BIGINT, DT DATE)
        USING iceberg
        PARTITIONED BY (DT)
        TBLPROPERTIES ('x' = 'y')
        """
        assert _parse_partition_spec(ddl) == ["dt"]

    def test_partition_before_location(self):
        ddl = """
        CREATE TABLE t (id BIGINT, dt DATE)
        USING iceberg
        PARTITIONED BY (dt)
        LOCATION '/tmp/data'
        """
        assert _parse_partition_spec(ddl) == ["dt"]

    def test_nested_transforms(self):
        ddl = """
        CREATE TABLE t (id BIGINT, ts TIMESTAMP)
        USING iceberg
        PARTITIONED BY (bucket(16, id), hours(ts))
        OPTIONS ('key' = 'value')
        """
        assert _parse_partition_spec(ddl) == ["bucket(16, id)", "hours(ts)"]


class TestNormalizeModelPartitions:
    def test_none(self):
        assert _normalize_model_partitions(None) == []

    def test_single_string(self):
        assert _normalize_model_partitions("dt") == ["dt"]

    def test_list(self):
        assert _normalize_model_partitions(["region", "dt"]) == ["region", "dt"]

    def test_case_normalized(self):
        assert _normalize_model_partitions(["DT", "Region"]) == ["dt", "region"]

    def test_whitespace_stripped(self):
        assert _normalize_model_partitions(["  dt  ", " region "]) == ["dt", "region"]

    def test_transform_expressions(self):
        assert _normalize_model_partitions(["days(ts)", "bucket(16, id)"]) == [
            "days(ts)",
            "bucket(16, id)",
        ]


class TestPartitionConfig:
    def test_equality_same(self):
        a = PartitionConfig(partition_columns=["dt", "region"])
        b = PartitionConfig(partition_columns=["dt", "region"])
        assert a == b

    def test_inequality_different_order(self):
        a = PartitionConfig(partition_columns=["dt", "region"])
        b = PartitionConfig(partition_columns=["region", "dt"])
        assert a != b

    def test_inequality_different_content(self):
        a = PartitionConfig(partition_columns=["dt"])
        b = PartitionConfig(partition_columns=["region"])
        assert a != b

    def test_equality_empty(self):
        a = PartitionConfig(partition_columns=[])
        b = PartitionConfig(partition_columns=[])
        assert a == b


class TestPartitionProcessor:
    def test_from_model_config_none(self):
        config = PartitionProcessor.from_model_config(None)
        assert config.partition_columns == []

    def test_from_model_config_string(self):
        config = PartitionProcessor.from_model_config("dt")
        assert config.partition_columns == ["dt"]

    def test_from_model_config_list(self):
        config = PartitionProcessor.from_model_config(["region", "days(ts)"])
        assert config.partition_columns == ["region", "days(ts)"]

    def test_from_show_create_table_none(self):
        with pytest.raises(DbtRuntimeError, match="partition metadata"):
            PartitionProcessor.from_show_create_table(None)

    def test_from_show_create_table_parses_ddl(self):
        ddl = (
            "CREATE TABLE cat.schema.tbl (id BIGINT, ts TIMESTAMP) "
            "USING iceberg "
            "PARTITIONED BY (days(ts), bucket(16, id)) "
            "TBLPROPERTIES ('format-version' = '2')"
        )
        config = PartitionProcessor.from_show_create_table(agate.Table([(ddl,)], ["ddl"]))
        assert config.partition_columns == ["days(ts)", "bucket(16, id)"]

    def test_is_out_of_sync_true(self):
        desired = PartitionConfig(partition_columns=["dt", "region"])
        existing = PartitionConfig(partition_columns=["dt"])
        assert PartitionProcessor.is_out_of_sync(desired, existing) is True

    def test_is_out_of_sync_false(self):
        desired = PartitionConfig(partition_columns=["dt"])
        existing = PartitionConfig(partition_columns=["dt"])
        assert PartitionProcessor.is_out_of_sync(desired, existing) is False

    def test_describe_diff(self):
        desired = PartitionConfig(partition_columns=["dt", "region"])
        existing = PartitionConfig(partition_columns=["dt"])
        diff = PartitionProcessor.describe_diff(desired, existing)
        assert "['dt', 'region']" in diff
        assert "['dt']" in diff


@pytest.mark.parametrize(
    "configured,actual",
    [
        ("`id`", "id"),
        ("bucket(16,   `id`)", "bucket(16,id)"),
        ("truncate(4, `region`)", "truncate(4,region)"),
        ("days( `Event_Time` )", "days(event_time)"),
        ("`a,b`", "`a,b`"),
        ("`a.b`", "`a.b`"),
        ("`a``b`", "`a``b`"),
        ("`two  spaces`", "`two  spaces`"),
    ],
)
def test_model_and_database_share_partition_normalization(configured, actual):
    assert _normalize_model_partitions(configured) == _parse_partition_spec(
        f"CREATE TABLE t USING iceberg PARTITIONED BY ({actual})"
    )


@pytest.mark.parametrize(
    "configured,actual",
    [
        ("`a,b`", "a, b"),
        ("`a.b`", "a.b"),
        ("`two  spaces`", "`two spaces`"),
        ("bucket(16,id)", "bucket(32,id)"),
        ("truncate(4,region)", "truncate(8,region)"),
    ],
)
def test_partition_normalization_preserves_real_differences(configured, actual):
    assert _normalize_model_partitions(configured) != _parse_partition_spec(
        f"CREATE TABLE t USING iceberg PARTITIONED BY ({actual})"
    )


def test_partition_identifiers_follow_case_sensitive_resolution():
    assert _normalize_model_partitions("`ID`", case_sensitive=True) == ["ID"]
    assert _normalize_model_partitions("ID", case_sensitive=True) != _parse_partition_spec(
        "CREATE TABLE t USING iceberg PARTITIONED BY (`id`)", case_sensitive=True
    )


@pytest.mark.parametrize(
    "value",
    [
        42,
        {},
        [None],
        [1],
        [""],
        " ",
        "id, other",
        "bucket(, id)",
        "bucket(0, id)",
        "bucket('16', id)",
        "truncate(-1, region)",
        "unknown(id)",
        "days()",
        "id STRING",
        "`unterminated",
        "id) TBLPROPERTIES ('x' = 'y') --",
    ],
)
def test_invalid_partition_config_raises(value):
    with pytest.raises(DbtRuntimeError, match="partition"):
        _normalize_model_partitions(value)


@pytest.mark.parametrize(
    "ddl",
    [
        "",
        "garbage",
        "CREATE TABLE t USING iceberg PARTITIONED BY (",
        "CREATE TABLE t USING iceberg PARTITIONED BY ()",
        "CREATE TABLE t USING iceberg PARTITIONED BY (id,)",
        "CREATE TABLE t USING iceberg PARTITIONED BY (`unterminated)",
        "CREATE TABLE t USING iceberg PARTITIONED BY (bucket(0, id))",
    ],
)
def test_invalid_partition_metadata_raises(ddl):
    with pytest.raises(DbtRuntimeError, match="partition"):
        _parse_partition_spec(ddl)


def test_partition_clause_text_inside_comment_is_not_a_partition():
    assert (
        _parse_partition_spec(
            "CREATE TABLE t (id BIGINT) USING iceberg COMMENT 'PARTITIONED BY (bad)'"
        )
        == []
    )


@pytest.mark.parametrize(
    "rows,columns",
    [
        ([], ["ddl"]),
        ([(None,)], ["ddl"]),
        ([("",)], ["ddl"]),
        ([()], []),
        ([("one", "two")], ["a", "b"]),
        ([("one",), ("two",)], ["ddl"]),
    ],
)
def test_invalid_show_create_result_shape_is_an_explicit_error(rows, columns):
    with pytest.raises(DbtRuntimeError, match="partition metadata"):
        PartitionProcessor.from_show_create_table(agate.Table(rows, columns))
