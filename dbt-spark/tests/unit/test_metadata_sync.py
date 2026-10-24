"""Unit tests for microbatch-safe metadata sync and diff-based persist_docs."""

import threading
from multiprocessing import get_context
from unittest import mock

import pytest
from agate import Row

from dbt.adapters.spark import SparkAdapter, SparkRelation

KEYS = ["col_name", "data_type", "comment"]


def _rows(plain_rows):
    return [Row(keys=KEYS, values=r) for r in plain_rows]


def _describe_rows(table_comment=None, col1_comment=None):
    plain = [
        ("col1", "string", col1_comment),
        ("dt", "date", None),
        ("# Partition Information", "", ""),
        ("# col_name", "data_type", "comment"),
        ("dt", "date", None),
        ("", "", ""),
        ("# Detailed Table Information", "", ""),
        ("Name", "db.tbl", ""),
        ("Owner", "root", ""),
    ]
    if table_comment is not None:
        plain.append(("Comment", table_comment, ""))
    plain.append(("Provider", "iceberg", ""))
    return _rows(plain)


@pytest.fixture
def adapter(target_http):
    return SparkAdapter(target_http, get_context("spawn"))


@pytest.fixture
def relation():
    return SparkRelation.create(schema="db", identifier="tbl", type="table")


class TestParseTableComment:
    def test_comment_present(self):
        assert SparkAdapter._parse_table_comment(_describe_rows("hello")) == "hello"

    def test_comment_absent(self):
        assert SparkAdapter._parse_table_comment(_describe_rows()) == ""

    def test_ignores_column_named_comment(self):
        rows = _rows(
            [
                ("Comment", "string", None),
                ("", "", ""),
                ("# Detailed Table Information", "", ""),
                ("Owner", "root", ""),
            ]
        )
        assert SparkAdapter._parse_table_comment(rows) == ""


class TestGetPersistDocsDiff:
    def _diff(self, adapter, relation, rows, description, columns, for_relation, for_columns):
        with mock.patch.object(adapter, "execute_macro", return_value=rows) as execute_macro:
            diff = adapter.get_persist_docs_diff(
                relation, description, columns, for_relation, for_columns
            )
        return diff, execute_macro

    def test_nothing_requested_skips_describe(self, adapter, relation):
        diff, execute_macro = self._diff(adapter, relation, [], "desc", {}, False, False)
        assert diff == {"relation_comment": None, "columns": {}}
        execute_macro.assert_not_called()

    def test_relation_comment_unchanged(self, adapter, relation):
        diff, _ = self._diff(adapter, relation, _describe_rows("same"), "same", {}, True, False)
        assert diff["relation_comment"] is None

    def test_relation_comment_changed(self, adapter, relation):
        diff, _ = self._diff(adapter, relation, _describe_rows("old"), "new", {}, True, False)
        assert diff["relation_comment"] == "new"

    def test_relation_comment_added(self, adapter, relation):
        diff, _ = self._diff(adapter, relation, _describe_rows(), "new", {}, True, False)
        assert diff["relation_comment"] == "new"

    def test_relation_comment_cleared(self, adapter, relation):
        diff, _ = self._diff(adapter, relation, _describe_rows("old"), "", {}, True, False)
        assert diff["relation_comment"] == ""

    def test_view_skips_relation_comment(self, adapter):
        view = SparkRelation.create(schema="db", identifier="v", type="view")
        diff, execute_macro = self._diff(
            adapter, view, _describe_rows("old"), "new", {}, True, False
        )
        assert diff["relation_comment"] is None
        execute_macro.assert_not_called()

    def test_columns_and_relation_single_describe(self, adapter, relation):
        columns = {
            "col1": {"name": "col1", "description": "new col"},
            "dt": {"name": "dt", "description": ""},
        }
        diff, execute_macro = self._diff(
            adapter, relation, _describe_rows("same", "old col"), "same", columns, True, True
        )
        assert diff["relation_comment"] is None
        assert diff["columns"] == {"col1": columns["col1"]}
        execute_macro.assert_called_once()


class TestClaimMetadataSync:
    def test_first_claim_wins(self, adapter, relation):
        assert adapter.claim_metadata_sync(relation, "inv-1") is True
        assert adapter.claim_metadata_sync(relation, "inv-1") is False

    def test_new_invocation_can_claim(self, adapter, relation):
        assert adapter.claim_metadata_sync(relation, "inv-1") is True
        assert adapter.claim_metadata_sync(relation, "inv-2") is True

    def test_other_relation_can_claim(self, adapter, relation):
        other = SparkRelation.create(schema="db", identifier="other", type="table")
        assert adapter.claim_metadata_sync(relation, "inv-1") is True
        assert adapter.claim_metadata_sync(other, "inv-1") is True

    def test_concurrent_claims_single_winner(self, adapter, relation):
        results = []
        barrier = threading.Barrier(16)

        def claim():
            barrier.wait()
            results.append(adapter.claim_metadata_sync(relation, "inv-1"))

        threads = [threading.Thread(target=claim) for _ in range(16)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert results.count(True) == 1
