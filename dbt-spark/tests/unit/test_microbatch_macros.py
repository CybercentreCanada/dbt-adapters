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


if __name__ == "__main__":
    unittest.main()
