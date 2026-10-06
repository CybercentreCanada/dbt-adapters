"""Unit tests for the microbatch-related macros in dbt-spark.

These cover the SQL-rendering / validation logic only — no live database
connection is required.
"""

import unittest
from unittest import mock

from jinja2 import Environment, FileSystemLoader


class _CompilerError(Exception):
    """Stand-in for dbt's `exceptions.raise_compiler_error`."""


def _raise_compiler_error(msg):
    raise _CompilerError(msg)


class TestMicrobatchMacros(unittest.TestCase):
    def setUp(self):
        self.jinja_env = Environment(
            loader=FileSystemLoader("src/dbt/include/spark/macros"),
            extensions=["jinja2.ext.do"],
        )

        self.config = {}
        self.target = mock.Mock()
        self.target.endpoint = None

        self.default_context = {
            "validation": mock.Mock(),
            "model": mock.Mock(),
            "exceptions": mock.Mock(),
            "config": mock.Mock(),
            "adapter": mock.Mock(),
            "target": self.target,
            "return": lambda r: r,
        }
        self.default_context["config"].get = lambda key, default=None, **kw: self.config.get(
            key, default
        )
        self.default_context["exceptions"].raise_compiler_error = _raise_compiler_error

    def _get_template(self, name):
        return self.jinja_env.get_template(name, globals=self.default_context)

    # -- validate.sql --------------------------------------------------------

    def test_validate_microbatch_rejects_non_iceberg_file_format(self):
        template = self._get_template("materializations/incremental/validate.sql")
        with self.assertRaises(_CompilerError) as ctx:
            template.module.dbt_spark_validate_get_incremental_strategy("microbatch", "parquet")
        self.assertIn("microbatch", str(ctx.exception).lower())
        self.assertIn("iceberg", str(ctx.exception).lower())

    def test_validate_microbatch_accepts_iceberg(self):
        template = self._get_template("materializations/incremental/validate.sql")
        # Should not raise.
        template.module.dbt_spark_validate_get_incremental_strategy("microbatch", "iceberg")

    def test_validate_other_strategies_unaffected_by_microbatch_guard(self):
        template = self._get_template("materializations/incremental/validate.sql")
        # insert_overwrite on parquet must still be allowed (microbatch guard
        # only fires for the microbatch strategy). Should not raise.
        template.module.dbt_spark_validate_get_incremental_strategy("insert_overwrite", "parquet")

    # -- strategies.sql ------------------------------------------------------

    def test_strategies_microbatch_rejects_non_iceberg_file_format(self):
        template = self._get_template("materializations/incremental/strategies.sql")
        self.config["file_format"] = "parquet"
        self.config["partition_by"] = ["date_day"]
        with self.assertRaises(_CompilerError) as ctx:
            template.module.dbt_spark_get_incremental_sql(
                "microbatch", "src", "tgt", mock.Mock(is_iceberg=False), None, None
            )
        self.assertIn("iceberg", str(ctx.exception).lower())

    def test_strategies_microbatch_requires_partition_by(self):
        template = self._get_template("materializations/incremental/strategies.sql")
        self.config["file_format"] = "iceberg"
        # partition_by deliberately unset
        with self.assertRaises(_CompilerError) as ctx:
            template.module.dbt_spark_get_incremental_sql(
                "microbatch", "src", "tgt", mock.Mock(is_iceberg=True), None, None
            )
        self.assertIn("partition_by", str(ctx.exception))

    # -- incremental.sql -----------------------------------------------------

    def test_incremental_gates_metadata_sync_on_first_batch_claim(self):
        with open(
            "src/dbt/include/spark/macros/materializations/incremental/incremental.sql"
        ) as f:
            source = f.read()
        self.assertNotIn("is_concurrent_microbatch", source)
        self.assertIn(
            "sync_metadata = (not model.batch) or "
            "adapter.claim_metadata_sync(target_relation, invocation_id)",
            source,
        )
        gate = source.index("{%- if sync_metadata -%}")
        self.assertLess(gate, source.index("adapter.check_partition_sync"))
        self.assertLess(gate, source.index("sync_tblproperties(target_relation"))
        self.assertIn(
            "{% if sync_metadata %}\n    {% do persist_docs(target_relation, model) %}", source
        )

    # -- adapters.sql: persist_docs ------------------------------------------

    def _render_persist_docs(self, diff, persist_relation=True, persist_columns=True):
        calls = {"relation": [], "columns": []}
        self.default_context["alter_relation_comment"] = lambda r, c: calls["relation"].append(c)
        self.default_context["alter_column_comment"] = lambda r, c: calls["columns"].append(c)
        config = self.default_context["config"]
        config.persist_relation_docs = lambda: persist_relation
        config.persist_column_docs = lambda: persist_columns
        adapter = self.default_context["adapter"]
        adapter.get_persist_docs_diff = mock.Mock(return_value=diff)
        model = mock.Mock(description="desc", columns={"c": {"description": "d"}})
        template = self._get_template("adapters.sql")
        template.module.spark__persist_docs("rel", model, True, True)
        return adapter.get_persist_docs_diff, calls

    def test_persist_docs_no_changes_emits_nothing(self):
        _, calls = self._render_persist_docs({"relation_comment": None, "columns": {}})
        self.assertEqual(calls, {"relation": [], "columns": []})

    def test_persist_docs_applies_only_diff(self):
        cols = {"c": {"description": "d"}}
        _, calls = self._render_persist_docs({"relation_comment": "new", "columns": cols})
        self.assertEqual(calls, {"relation": ["new"], "columns": [cols]})

    def test_persist_docs_disabled_skips_describe(self):
        get_diff, calls = self._render_persist_docs(
            {"relation_comment": "new", "columns": {}},
            persist_relation=False,
            persist_columns=False,
        )
        get_diff.assert_not_called()
        self.assertEqual(calls, {"relation": [], "columns": []})


if __name__ == "__main__":
    unittest.main()
