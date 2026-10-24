import unittest
from unittest import mock
import re
import sys
import warnings
from pathlib import Path
from types import SimpleNamespace
from jinja2 import Environment, FileSystemLoader


class TestSparkMacros(unittest.TestCase):
    def setUp(self):
        self.jinja_env = Environment(
            loader=FileSystemLoader("src/dbt/include/spark/macros"),
            extensions=[
                "jinja2.ext.do",
            ],
        )

        self.config = {}
        self.default_context = {
            "validation": mock.Mock(),
            "model": mock.Mock(),
            "exceptions": mock.Mock(),
            "config": mock.Mock(),
            "adapter": mock.Mock(),
            "return": lambda r: r,
            "spark__filtered_tblproperties": lambda props: props if props else None,
            "spark__escape_single_quotes": lambda expr: expr,
        }
        self.default_context["config"].get = lambda key, default=None, **kwargs: self.config.get(
            key, default
        )
        self.default_context["adapter"].behavior.require_location_root = False

    def __get_template(self, template_filename):
        return self.jinja_env.get_template(template_filename, globals=self.default_context)

    def __run_macro(self, template, name, temporary, relation, sql):
        self.default_context["model"].alias = relation

        def dispatch(macro_name, macro_namespace=None, packages=None):
            return getattr(template.module, f"spark__{macro_name}")

        self.default_context["adapter"].dispatch = dispatch

        value = getattr(template.module, name)(temporary, relation, sql)
        return re.sub(r"\s\s+", " ", value)

    def test_macros_load(self):
        self.jinja_env.get_template("adapters.sql")

    def test_table_rejects_streaming_only_options(self):
        source = (
            Path("src/dbt/include/spark/macros/materializations/table.sql")
            .read_text()
            .replace(
                "{% materialization table, adapter = 'spark', supported_languages=['sql', 'python'] %}",
                "{% macro table() %}",
            )
            .replace("{% endmaterialization %}", "{% endmacro %}")
        )
        environment = Environment(extensions=["jinja2.ext.do"])
        template = environment.from_string(source)

        def raise_compiler_error(message):
            raise ValueError(message)

        template.globals.update(
            {
                "config": type(
                    "Config",
                    (),
                    {
                        "get": lambda _, key, default=None, **kwargs: (
                            {"fanout-enabled": "true"}
                            if key == "write_stream_options"
                            else default
                        )
                    },
                )(),
                "exceptions": type(
                    "Exceptions", (), {"raise_compiler_error": staticmethod(raise_compiler_error)}
                )(),
                "spark__validate_streaming_options_config": lambda materialization: raise_compiler_error(
                    "'write_stream_options' is only supported for materialized='streaming'; "
                    f"remove it from this {materialization} model."
                ),
            }
        )

        with self.assertRaisesRegex(ValueError, "materialized='streaming'"):
            template.module.table()

    def test_macros_create_table_as(self):
        template = self.__get_template("adapters.sql")
        sql = self.__run_macro(
            template, "spark__create_table_as", False, "my_table", "select 1"
        ).strip()

        self.assertEqual(sql, "create or replace table my_table using iceberg as select 1")

    def test_macros_create_table_as_file_format(self):
        template = self.__get_template("adapters.sql")

        self.config["file_format"] = "delta"
        sql = self.__run_macro(
            template, "spark__create_table_as", False, "my_table", "select 1"
        ).strip()
        self.assertEqual(sql, "create or replace table my_table using delta as select 1")

        self.config["file_format"] = "hudi"
        sql = self.__run_macro(
            template, "spark__create_table_as", False, "my_table", "select 1"
        ).strip()
        self.assertEqual(sql, "create table my_table using hudi as select 1")

    def test_macros_create_table_as_options(self):
        template = self.__get_template("adapters.sql")

        self.config["file_format"] = "delta"
        self.config["options"] = {"compression": "gzip"}
        sql = self.__run_macro(
            template, "spark__create_table_as", False, "my_table", "select 1"
        ).strip()
        self.assertEqual(
            sql,
            'create or replace table my_table using delta options (compression "gzip" ) as select 1',
        )

        self.config["file_format"] = "hudi"
        sql = self.__run_macro(
            template, "spark__create_table_as", False, "my_table", "select 1"
        ).strip()
        self.assertEqual(
            sql, 'create table my_table using hudi options (compression "gzip" ) as select 1'
        )

    def test_macros_create_table_as_hudi_options(self):
        template = self.__get_template("adapters.sql")

        self.config["file_format"] = "hudi"
        self.config["unique_key"] = "id"
        sql = self.__run_macro(
            template, "spark__create_table_as", False, "my_table", "select 1 as id"
        ).strip()
        self.assertEqual(
            sql, 'create table my_table using hudi options (primaryKey "id" ) as select 1 as id'
        )

        self.config["file_format"] = "hudi"
        self.config["unique_key"] = "id"
        self.config["options"] = {"primaryKey": "id"}
        sql = self.__run_macro(
            template, "spark__create_table_as", False, "my_table", "select 1 as id"
        ).strip()
        self.assertEqual(
            sql, 'create table my_table using hudi options (primaryKey "id" ) as select 1 as id'
        )

        self.config["file_format"] = "hudi"
        self.config["unique_key"] = "uuid"
        self.config["options"] = {"primaryKey": "id"}
        sql = self.__run_macro(
            template, "spark__create_table_as", False, "my_table", "select 1 as id"
        )
        self.assertIn("mock.raise_compiler_error()", sql)

    def test_macros_create_table_as_partition(self):
        template = self.__get_template("adapters.sql")

        self.config["partition_by"] = "partition_1"
        sql = self.__run_macro(
            template, "spark__create_table_as", False, "my_table", "select 1"
        ).strip()
        self.assertEqual(
            sql,
            "create or replace table my_table using iceberg partitioned by (partition_1) as select 1",
        )

    def test_macros_create_table_as_partitions(self):
        template = self.__get_template("adapters.sql")

        self.config["partition_by"] = ["partition_1", "truncate(4, region_code)"]
        sql = self.__run_macro(
            template, "spark__create_table_as", False, "my_table", "select 1"
        ).strip()
        self.assertEqual(
            sql,
            "create or replace table my_table using iceberg partitioned by (partition_1,truncate(4, region_code)) as select 1",
        )

    def test_macros_create_table_as_cluster(self):
        template = self.__get_template("adapters.sql")

        self.config["clustered_by"] = "cluster_1"
        self.config["buckets"] = "1"
        sql = self.__run_macro(
            template, "spark__create_table_as", False, "my_table", "select 1"
        ).strip()
        self.assertEqual(
            sql,
            "create or replace table my_table using iceberg clustered by (cluster_1) into 1 buckets as select 1",
        )

    def test_macros_create_table_as_clusters(self):
        template = self.__get_template("adapters.sql")

        self.config["clustered_by"] = ["cluster_1", "cluster_2"]
        self.config["buckets"] = "1"
        sql = self.__run_macro(
            template, "spark__create_table_as", False, "my_table", "select 1"
        ).strip()
        self.assertEqual(
            sql,
            "create or replace table my_table using iceberg clustered by (cluster_1,cluster_2) into 1 buckets as select 1",
        )

    def test_macros_create_table_as_location(self):
        template = self.__get_template("adapters.sql")

        self.config["location_root"] = "/mnt/root"
        sql = self.__run_macro(
            template, "spark__create_table_as", False, "my_table", "select 1"
        ).strip()
        self.assertEqual(
            sql,
            "create or replace table my_table using iceberg location '/mnt/root/my_table' as select 1",
        )

    def test_python_location_clause(self):
        template = self.__get_template("adapters.sql")

        self.config["location_root"] = "/mnt/root"
        self.default_context["model"].alias = "my_table"
        python = template.module.python__location_clause()

        self.assertIn('target_location = "/mnt/root/my_table"', python)
        self.assertIn('writer = writer.option("location", target_location)', python)

    def __get_python_table_template(self):
        source = (
            Path("src/dbt/include/spark/macros/materializations/table.sql")
            .read_text()
            .replace(
                "{% materialization table, adapter = 'spark', supported_languages=['sql', 'python'] %}",
                "{% macro table() %}",
            )
            .replace("{% endmaterialization %}", "{% endmacro %}")
        )
        return self.jinja_env.from_string(source, globals=self.default_context)

    def test_python_partitioned_by_clause_quotes_transform_columns(self):
        template = self.__get_python_table_template()
        self.config["partition_by"] = [
            "region",
            "days(ingest_loaded_by)",
            "months(`created_at`)",
            "bucket(16, user_id)",
        ]
        python = template.module.python__partitionedBy_clause()

        self.assertIn(
            'writer = writer.partitionedBy("region",days("ingest_loaded_by"),months("created_at"),bucket(16, "user_id"))',
            python,
        )

    def test_python_table_partition_functions(self):
        self.default_context["python__location_clause"] = lambda: ""
        transforms = {
            "years": ("created_at",),
            "months": ("created_at",),
            "days": ("created_at",),
            "hours": ("ingest_loaded_by",),
            "bucket": (16, "user_id"),
        }
        partitions = [
            "region",
            "years(created_at)",
            "months(`created_at`)",
            "days(created_at)",
            "hours(ingest_loaded_by)",
            "bucket(16, user_id)",
        ]
        for modern in (True, False):
            for temporary in (True, False):
                for partition_by in (partitions, "hours(ingest_loaded_by)", None):
                    with self.subTest(
                        modern=modern, temporary=temporary, partition_by=partition_by
                    ):
                        functions = SimpleNamespace(
                            **{
                                name: mock.Mock(return_value=f"{name}_expression")
                                for name in transforms
                            }
                        )
                        if modern:
                            functions.partitioning = SimpleNamespace(
                                **{
                                    name: mock.Mock(return_value=f"{name}_expression")
                                    for name in transforms
                                }
                            )
                            for name in transforms:
                                getattr(functions, name).side_effect = AssertionError(
                                    f"Deprecated {name} helper called"
                                )
                        selected = functions.partitioning if modern else functions
                        dataframe_type = type("DataFrame", (), {})
                        df = dataframe_type()
                        df.createOrReplaceTempView = mock.Mock()
                        writer = mock.Mock()
                        writer.using.return_value = writer
                        writer.option.return_value = writer
                        writer.partitionedBy.return_value = writer
                        df.writeTo = mock.Mock(return_value=writer)
                        spark = mock.Mock()
                        pyspark = SimpleNamespace(
                            sql=SimpleNamespace(
                                dataframe=SimpleNamespace(DataFrame=dataframe_type),
                                functions=functions,
                            )
                        )
                        self.config["partition_by"] = partition_by
                        rendered = self.__get_python_table_template().module.py_write_table(
                            "", "catalog.schema.target", temporary
                        )
                        modules = {
                            "pyspark": pyspark,
                            "pyspark.sql": pyspark.sql,
                            "pyspark.sql.functions": functions,
                            "pyspark.pandas": None,
                            "pandas": None,
                            "databricks.koalas": None,
                        }
                        with mock.patch.dict(sys.modules, modules):
                            with warnings.catch_warnings():
                                warnings.simplefilter("error", FutureWarning)
                                exec(
                                    compile(rendered, "python_table.py", "exec"),
                                    {
                                        "spark": spark,
                                        "dbtObj": mock.Mock(),
                                        "model": mock.Mock(return_value=df),
                                    },
                                )
                        if temporary:
                            df.createOrReplaceTempView.assert_called_once_with(
                                "catalog.schema.target"
                            )
                            df.writeTo.assert_not_called()
                        else:
                            df.writeTo.assert_called_once_with("catalog.schema.target")
                            writer.create.assert_called_once_with()
                            df.createOrReplaceTempView.assert_not_called()
                        expected = (
                            list(transforms)
                            if partition_by == partitions
                            else ["hours"] if partition_by else []
                        )
                        for name, args in transforms.items():
                            helper = getattr(selected, name)
                            if not temporary and name in expected:
                                helper.assert_called_once_with(*args)
                            else:
                                helper.assert_not_called()
                            if modern:
                                getattr(functions, name).assert_not_called()
                        if not temporary and expected:
                            expressions = [f"{name}_expression" for name in expected]
                            if partition_by == partitions:
                                expressions.insert(0, "region")
                            writer.partitionedBy.assert_called_once_with(*expressions)
                        else:
                            writer.partitionedBy.assert_not_called()

    def test_macros_create_table_as_comment(self):
        template = self.__get_template("adapters.sql")

        self.config["persist_docs"] = {"relation": True}
        self.default_context["model"].description = "Description Test"
        sql = self.__run_macro(
            template, "spark__create_table_as", False, "my_table", "select 1"
        ).strip()
        self.assertEqual(
            sql,
            "create or replace table my_table using iceberg comment 'Description Test' as select 1",
        )

    def test_macros_create_table_as_all(self):
        template = self.__get_template("adapters.sql")

        self.config["file_format"] = "delta"
        self.config["location_root"] = "/mnt/root"
        self.config["partition_by"] = ["partition_1", "partition_2"]
        self.config["clustered_by"] = ["cluster_1", "cluster_2"]
        self.config["buckets"] = "1"
        self.config["persist_docs"] = {"relation": True}
        self.default_context["model"].description = "Description Test"

        sql = self.__run_macro(
            template, "spark__create_table_as", False, "my_table", "select 1"
        ).strip()
        self.assertEqual(
            sql,
            "create or replace table my_table using delta partitioned by (partition_1,partition_2) clustered by (cluster_1,cluster_2) into 1 buckets location '/mnt/root/my_table' comment 'Description Test' as select 1",
        )

        self.config["file_format"] = "hudi"
        sql = self.__run_macro(
            template, "spark__create_table_as", False, "my_table", "select 1"
        ).strip()
        self.assertEqual(
            sql,
            "create table my_table using hudi partitioned by (partition_1,partition_2) clustered by (cluster_1,cluster_2) into 1 buckets location '/mnt/root/my_table' comment 'Description Test' as select 1",
        )
