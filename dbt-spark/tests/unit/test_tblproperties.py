"""Unit tests for TblPropertiesConfig, TblPropertiesProcessor, and TblPropertiesDiff."""

import pytest

from dbt.adapters.spark.relation_configs.tblproperties import (
    TblPropertiesConfig,
    TblPropertiesDiff,
    TblPropertiesProcessor,
    _filter_properties,
)


class TestFilterProperties:
    def test_removes_polaris_prefix(self):
        props = {"polaris_foo": "bar", "custom_key": "val", "polaris_": "x"}
        assert _filter_properties(props) == {"custom_key": "val"}

    def test_empty_dict(self):
        assert _filter_properties({}) == {}

    def test_no_polaris_keys(self):
        props = {"a": "1", "b": "2"}
        assert _filter_properties(props) == props


class TestTblPropertiesConfig:
    def test_equality_ignores_polaris(self):
        a = TblPropertiesConfig(tblproperties={"key": "val", "polaris_x": "y"})
        b = TblPropertiesConfig(tblproperties={"key": "val", "polaris_z": "w"})
        assert a == b

    def test_inequality_on_user_props(self):
        a = TblPropertiesConfig(tblproperties={"key": "val1"})
        b = TblPropertiesConfig(tblproperties={"key": "val2"})
        assert a != b

    def test_hash_ignores_polaris(self):
        a = TblPropertiesConfig(tblproperties={"key": "val", "polaris_x": "y"})
        b = TblPropertiesConfig(tblproperties={"key": "val"})
        assert hash(a) == hash(b)


class TestTblPropertiesProcessor:
    def test_from_relation_config_none(self):
        config = TblPropertiesProcessor.from_relation_config(None)
        assert config.tblproperties == {}

    def test_from_relation_config_filters_polaris(self):
        config = TblPropertiesProcessor.from_relation_config(
            {"a": "1", "polaris_managed": "true"}
        )
        assert config.tblproperties == {"a": "1"}

    def test_from_relation_results_empty(self):
        config = TblPropertiesProcessor.from_relation_results(None)
        assert config.tblproperties == {}

    def test_from_relation_results_parses_rows(self):
        class FakeRow:
            def __init__(self, key, value):
                self._data = (key, value)

            def __getitem__(self, idx):
                return self._data[idx]

        class FakeTable:
            def __init__(self, rows):
                self.rows = rows

        table = FakeTable([FakeRow("k1", "v1"), FakeRow("polaris_x", "px"), FakeRow("k2", "v2")])
        config = TblPropertiesProcessor.from_relation_results(table)
        assert config.tblproperties == {"k1": "v1", "k2": "v2"}


class TestGetDiff:
    def test_no_changes(self):
        desired = TblPropertiesConfig(tblproperties={"a": "1"})
        existing = TblPropertiesConfig(tblproperties={"a": "1"})
        assert TblPropertiesProcessor.get_diff(desired, existing) is None

    def test_no_changes_ignores_polaris(self):
        desired = TblPropertiesConfig(tblproperties={"a": "1"})
        existing = TblPropertiesConfig(tblproperties={"a": "1", "polaris_foo": "bar"})
        assert TblPropertiesProcessor.get_diff(desired, existing) is None

    def test_set_new_property(self):
        desired = TblPropertiesConfig(tblproperties={"a": "1", "b": "2"})
        existing = TblPropertiesConfig(tblproperties={"a": "1"})
        diff = TblPropertiesProcessor.get_diff(desired, existing)
        assert diff is not None
        assert diff.set_properties == {"b": "2"}
        assert diff.unset_properties == []

    def test_update_existing_property(self):
        desired = TblPropertiesConfig(tblproperties={"a": "new"})
        existing = TblPropertiesConfig(tblproperties={"a": "old"})
        diff = TblPropertiesProcessor.get_diff(desired, existing)
        assert diff is not None
        assert diff.set_properties == {"a": "new"}
        assert diff.unset_properties == []

    def test_unset_removed_property(self):
        desired = TblPropertiesConfig(tblproperties={"a": "1"})
        existing = TblPropertiesConfig(tblproperties={"a": "1", "b": "2"})
        diff = TblPropertiesProcessor.get_diff(desired, existing)
        assert diff is not None
        assert diff.set_properties == {}
        assert diff.unset_properties == ["b"]

    def test_combined_set_and_unset(self):
        desired = TblPropertiesConfig(tblproperties={"a": "1", "c": "3"})
        existing = TblPropertiesConfig(tblproperties={"a": "1", "b": "2"})
        diff = TblPropertiesProcessor.get_diff(desired, existing)
        assert diff is not None
        assert diff.set_properties == {"c": "3"}
        assert diff.unset_properties == ["b"]

    def test_polaris_in_existing_not_unset(self):
        """polaris_ properties in existing should not appear in unset list."""
        desired = TblPropertiesConfig(tblproperties={})
        existing = TblPropertiesConfig(tblproperties={"polaris_x": "y"})
        assert TblPropertiesProcessor.get_diff(desired, existing) is None


class TestTblPropertiesDiff:
    def test_has_changes_false(self):
        diff = TblPropertiesDiff()
        assert diff.has_changes is False

    def test_has_changes_with_set(self):
        diff = TblPropertiesDiff(set_properties={"a": "1"})
        assert diff.has_changes is True

    def test_has_changes_with_unset(self):
        diff = TblPropertiesDiff(unset_properties=["a"])
        assert diff.has_changes is True
