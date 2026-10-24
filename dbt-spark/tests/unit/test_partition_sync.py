"""Unit tests for partition drift detection logic."""

import pytest

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
        assert _parse_partition_spec(ddl) == ["my column"]

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
        config = PartitionProcessor.from_show_create_table(None)
        assert config.partition_columns == []

    def test_from_show_create_table_parses_ddl(self):
        class FakeRow:
            def __init__(self, val):
                self._val = val

            def __getitem__(self, idx):
                return self._val

        class FakeTable:
            def __init__(self, ddl):
                self.rows = [FakeRow(ddl)]

        ddl = (
            "CREATE TABLE cat.schema.tbl (id BIGINT, ts TIMESTAMP) "
            "USING iceberg "
            "PARTITIONED BY (days(ts), bucket(16, id)) "
            "TBLPROPERTIES ('format-version' = '2')"
        )
        config = PartitionProcessor.from_show_create_table(FakeTable(ddl))
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
