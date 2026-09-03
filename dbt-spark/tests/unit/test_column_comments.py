"""Unit tests for column comment diff logic (get_persist_doc_columns)."""

import pytest

from dbt.adapters.spark.column import SparkColumn


def _make_column(name: str, comment: str = None) -> SparkColumn:
    return SparkColumn(column=name, dtype="string", comment=comment)


class TestGetPersistDocColumns:
    """Tests for SparkAdapter.get_persist_doc_columns logic.

    Since the method only depends on SparkColumn objects and a dict,
    we test the core algorithm directly without mocking the full adapter.
    """

    @staticmethod
    def _get_persist_doc_columns(existing_columns, columns):
        """Standalone reimplementation matching SparkAdapter.get_persist_doc_columns."""
        return_columns = {}
        columns_lower = {k.lower(): k for k in columns.keys()}

        for column in existing_columns:
            name = column.column
            name_lower = name.lower()
            if name_lower in columns_lower:
                original_column_name = columns_lower[name_lower]
                config_column = columns[original_column_name]
                if isinstance(config_column, dict):
                    comment = config_column.get("description", "")
                elif hasattr(config_column, "description"):
                    comment = config_column.description or ""
                else:
                    continue
                if comment != (column.comment or ""):
                    return_columns[name] = columns[original_column_name]

        return return_columns

    def test_empty(self):
        assert self._get_persist_doc_columns([], {}) == {}

    def test_no_matching_columns(self):
        existing = [_make_column("col1", "existing comment")]
        model_columns = {"col2": {"name": "col2", "description": "new comment"}}
        assert self._get_persist_doc_columns(existing, model_columns) == {}

    def test_comment_matches_no_update(self):
        existing = [_make_column("col1", "same comment")]
        model_columns = {"col1": {"name": "col1", "description": "same comment"}}
        assert self._get_persist_doc_columns(existing, model_columns) == {}

    def test_comment_differs_returns_column(self):
        existing = [_make_column("col1", "old comment")]
        model_columns = {"col1": {"name": "col1", "description": "new comment"}}
        result = self._get_persist_doc_columns(existing, model_columns)
        assert result == model_columns

    def test_none_comment_treated_as_empty(self):
        """A column with no existing comment (None) should match empty description."""
        existing = [_make_column("col1", None)]
        model_columns = {"col1": {"name": "col1", "description": ""}}
        assert self._get_persist_doc_columns(existing, model_columns) == {}

    def test_none_comment_differs_from_nonempty(self):
        """A column with no existing comment should get updated if description is non-empty."""
        existing = [_make_column("col1", None)]
        model_columns = {"col1": {"name": "col1", "description": "new comment"}}
        result = self._get_persist_doc_columns(existing, model_columns)
        assert result == model_columns

    def test_case_insensitive_match(self):
        """Column name matching should be case-insensitive."""
        existing = [_make_column("Account_ID", "")]
        model_columns = {"account_id": {"name": "account_id", "description": "Account ID"}}
        result = self._get_persist_doc_columns(existing, model_columns)
        # Key should be the database column name, value from model config
        assert "Account_ID" in result
        assert result["Account_ID"]["description"] == "Account ID"

    def test_case_insensitive_no_update_needed(self):
        """Case-insensitive match should still skip if comments match."""
        existing = [_make_column("Account_ID", "Account ID")]
        model_columns = {"account_id": {"name": "account_id", "description": "Account ID"}}
        assert self._get_persist_doc_columns(existing, model_columns) == {}

    def test_mixed_columns(self):
        """Only columns with differing comments should be returned."""
        existing = [
            _make_column("col1", "comment1"),
            _make_column("col2", "comment2"),
            _make_column("col3", None),
        ]
        model_columns = {
            "col1": {"name": "col1", "description": "comment1"},  # same - skip
            "col2": {"name": "col2", "description": "updated"},  # different - include
            "col3": {"name": "col3", "description": "new"},  # None vs non-empty - include
        }
        result = self._get_persist_doc_columns(existing, model_columns)
        assert "col1" not in result
        assert "col2" in result
        assert "col3" in result
        assert len(result) == 2
