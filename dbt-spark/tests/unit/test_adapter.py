import unittest
from multiprocessing import get_context
from unittest import mock

import pytest
from dbt_common.exceptions import DbtRuntimeError
from agate import Row
from pyhive import hive
from dbt.adapters.spark import SparkAdapter, SparkColumn, SparkRelation
from dbt.adapters.spark.impl import (
    LIST_RELATIONS_MACRO_NAME,
    SCHEMA_NOT_FOUND_MESSAGES,
    TABLE_OR_VIEW_NOT_FOUND_MESSAGES,
)
from dbt.adapters.spark.connections import SparkConnectionMethod
from dbt.adapters.contracts.relation import RelationType
from .utils import config_from_parts_or_dicts

ENFORCED_SPARK_CONFIG = {"spark.sql.ansi.enabled": "false"}


class TestSparkAdapter(unittest.TestCase):
    @pytest.fixture(autouse=True)
    def set_up_fixtures(
        self,
        target_http,
        target_odbc_with_extra_conn,
        target_thrift,
        target_thrift_kerberos,
        target_odbc_sql_endpoint,
        target_odbc_cluster,
        target_use_ssl_thrift,
        base_project_cfg,
    ):
        self.base_project_cfg = base_project_cfg
        self.target_http = target_http
        self.target_odbc_with_extra_conn = target_odbc_with_extra_conn
        self.target_odbc_sql_endpoint = target_odbc_sql_endpoint
        self.target_odbc_cluster = target_odbc_cluster
        self.target_thrift = target_thrift
        self.target_thrift_kerberos = target_thrift_kerberos
        self.target_use_ssl_thrift = target_use_ssl_thrift

    def test_http_connection(self):
        adapter = SparkAdapter(self.target_http, get_context("spawn"))

        def hive_http_connect(thrift_transport, configuration):
            self.assertEqual(thrift_transport.scheme, "https")
            self.assertEqual(thrift_transport.port, 443)
            self.assertEqual(thrift_transport.host, "myorg.sparkhost.com")
            self.assertEqual(
                thrift_transport.path, "/sql/protocolv1/o/0123456789/01234-23423-coffeetime"
            )
            self.assertEqual(configuration["spark.driver.memory"], "4g")

        # with mock.patch.object(hive, 'connect', new=hive_http_connect):
        with mock.patch("dbt.adapters.spark.connections.hive.connect", new=hive_http_connect):
            connection = adapter.acquire_connection("dummy")
            connection.handle  # trigger lazy-load

            self.assertEqual(connection.state, "open")
            self.assertIsNotNone(connection.handle)
            self.assertEqual(connection.credentials.cluster, "01234-23423-coffeetime")
            self.assertEqual(connection.credentials.token, "abc123")
            self.assertEqual(connection.credentials.schema, "analytics")
            self.assertIsNone(connection.credentials.database)

    def test_thrift_connection(self):
        config = self.target_thrift
        adapter = SparkAdapter(config, get_context("spawn"))

        def hive_thrift_connect(
            host, port, username, auth, kerberos_service_name, password, configuration
        ):
            self.assertEqual(host, "myorg.sparkhost.com")
            self.assertEqual(port, 10001)
            self.assertEqual(username, "dbt")
            self.assertIsNone(auth)
            self.assertIsNone(kerberos_service_name)
            self.assertIsNone(password)
            self.assertDictEqual(configuration, ENFORCED_SPARK_CONFIG)

        with mock.patch.object(hive, "connect", new=hive_thrift_connect):
            connection = adapter.acquire_connection("dummy")
            connection.handle  # trigger lazy-load

            self.assertEqual(connection.state, "open")
            self.assertIsNotNone(connection.handle)
            self.assertEqual(connection.credentials.schema, "analytics")
            self.assertIsNone(connection.credentials.database)

    def test_thrift_ssl_connection(self):
        adapter = SparkAdapter(self.target_use_ssl_thrift, get_context("spawn"))

        def hive_thrift_connect(thrift_transport, configuration):
            self.assertIsNotNone(thrift_transport)
            transport = thrift_transport._trans
            self.assertEqual(transport.host, "myorg.sparkhost.com")
            self.assertEqual(transport.port, 10001)
            self.assertDictEqual(configuration, ENFORCED_SPARK_CONFIG)

        with mock.patch.object(hive, "connect", new=hive_thrift_connect):
            connection = adapter.acquire_connection("dummy")
            connection.handle  # trigger lazy-load

            self.assertEqual(connection.state, "open")
            self.assertIsNotNone(connection.handle)
            self.assertEqual(connection.credentials.schema, "analytics")
            self.assertIsNone(connection.credentials.database)

    def test_thrift_connection_kerberos(self):
        adapter = SparkAdapter(self.target_thrift_kerberos, get_context("spawn"))

        def hive_thrift_connect(
            host, port, username, auth, kerberos_service_name, password, configuration
        ):
            self.assertEqual(host, "myorg.sparkhost.com")
            self.assertEqual(port, 10001)
            self.assertEqual(username, "dbt")
            self.assertEqual(auth, "KERBEROS")
            self.assertEqual(kerberos_service_name, "hive")
            self.assertIsNone(password)
            self.assertDictEqual(configuration, ENFORCED_SPARK_CONFIG)

        with mock.patch.object(hive, "connect", new=hive_thrift_connect):
            connection = adapter.acquire_connection("dummy")
            connection.handle  # trigger lazy-load

            self.assertEqual(connection.state, "open")
            self.assertIsNotNone(connection.handle)
            self.assertEqual(connection.credentials.schema, "analytics")
            self.assertIsNone(connection.credentials.database)

    def test_odbc_cluster_connection(self):
        adapter = SparkAdapter(self.target_odbc_cluster, get_context("spawn"))

        def pyodbc_connect(connection_str, autocommit):
            self.assertTrue(autocommit)
            self.assertIn("driver=simba;", connection_str.lower())
            self.assertIn("port=443;", connection_str.lower())
            self.assertIn("host=myorg.sparkhost.com;", connection_str.lower())
            self.assertIn(
                "httppath=/sql/protocolv1/o/0123456789/01234-23423-coffeetime;",
                connection_str.lower(),
            )  # noqa

        with mock.patch(
            "dbt.adapters.spark.connections.pyodbc.connect", new=pyodbc_connect
        ):  # noqa
            connection = adapter.acquire_connection("dummy")
            connection.handle  # trigger lazy-load

            self.assertEqual(connection.state, "open")
            self.assertIsNotNone(connection.handle)
            self.assertEqual(connection.credentials.cluster, "01234-23423-coffeetime")
            self.assertEqual(connection.credentials.token, "abc123")
            self.assertEqual(connection.credentials.schema, "analytics")
            self.assertIsNone(connection.credentials.database)

    def test_odbc_endpoint_connection(self):
        adapter = SparkAdapter(self.target_odbc_sql_endpoint, get_context("spawn"))

        def pyodbc_connect(connection_str, autocommit):
            self.assertTrue(autocommit)
            self.assertIn("driver=simba;", connection_str.lower())
            self.assertIn("port=443;", connection_str.lower())
            self.assertIn("host=myorg.sparkhost.com;", connection_str.lower())
            self.assertIn(
                "httppath=/sql/1.0/endpoints/012342342393920a;", connection_str.lower()
            )  # noqa

        with mock.patch(
            "dbt.adapters.spark.connections.pyodbc.connect", new=pyodbc_connect
        ):  # noqa
            connection = adapter.acquire_connection("dummy")
            connection.handle  # trigger lazy-load

            self.assertEqual(connection.state, "open")
            self.assertIsNotNone(connection.handle)
            self.assertEqual(connection.credentials.endpoint, "012342342393920a")
            self.assertEqual(connection.credentials.token, "abc123")
            self.assertEqual(connection.credentials.schema, "analytics")
            self.assertIsNone(connection.credentials.database)

    def test_odbc_with_extra_connection_string(self):
        adapter = SparkAdapter(self.target_odbc_with_extra_conn, get_context("spawn"))

        def pyodbc_connect(connection_str, autocommit):
            self.assertTrue(autocommit)
            self.assertIn("driver=simba;", connection_str.lower())
            self.assertIn("port=443;", connection_str.lower())
            self.assertIn("host=myorg.sparkhost.com;", connection_str.lower())
            self.assertIn("someExtraValues", connection_str)

        with mock.patch(
            "dbt.adapters.spark.connections.pyodbc.connect", new=pyodbc_connect
        ):  # noqa
            connection = adapter.acquire_connection("dummy")
            connection.handle  # trigger lazy-load

            self.assertEqual(connection.state, "open")
            self.assertIsNotNone(connection.handle)
            self.assertIsNone(connection.credentials.database)

    def test_parse_relation(self):
        self.maxDiff = None
        rel_type = SparkRelation.get_relation_type.Table

        relation = SparkRelation.create(
            schema="default_schema", identifier="mytable", type=rel_type
        )
        assert relation.database is None

        # Mimics the output of Spark with a DESCRIBE TABLE EXTENDED
        plain_rows = [
            ("col1", "decimal(22,0)", None),
            ("col2", "string", None),
            ("dt", "date", None),
            ("struct_col", "struct<struct_inner_col:string>", None),
            ("# Partition Information", "data_type", None),
            ("# col_name", "data_type", None),
            ("dt", "date", None),
            (None, None, None),
            ("# Detailed Table Information", None, None),
            ("Database", None, None),
            ("Owner", "root", None),
            ("Created Time", "Wed Feb 04 18:15:00 UTC 1815", None),
            ("Last Access", "Wed May 20 19:25:00 UTC 1925", None),
            ("Type", "MANAGED", None),
            ("Provider", "delta", None),
            ("Location", "/mnt/vo", None),
            ("Serde Library", "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe", None),
            ("InputFormat", "org.apache.hadoop.mapred.SequenceFileInputFormat", None),
            ("OutputFormat", "org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat", None),
            ("Partition Provider", "Catalog", None),
        ]

        input_cols = [Row(keys=["col_name", "data_type", "comment"], values=r) for r in plain_rows]

        rows = SparkAdapter(self.target_http, get_context("spawn")).parse_describe_extended(
            relation, input_cols
        )
        self.assertEqual(len(rows), 4)
        self.assertEqual(
            rows[0].to_column_dict(omit_none=False),
            {
                "table_database": None,
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "table_owner": "root",
                "column": "col1",
                "column_index": 0,
                "dtype": "decimal(22,0)",
                "numeric_scale": None,
                "numeric_precision": None,
                "char_size": None,
                "comment": None,
            },
        )

        self.assertEqual(
            rows[1].to_column_dict(omit_none=False),
            {
                "table_database": None,
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "table_owner": "root",
                "column": "col2",
                "column_index": 1,
                "dtype": "string",
                "numeric_scale": None,
                "numeric_precision": None,
                "char_size": None,
                "comment": None,
            },
        )

        self.assertEqual(
            rows[2].to_column_dict(omit_none=False),
            {
                "table_database": None,
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "table_owner": "root",
                "column": "dt",
                "column_index": 2,
                "dtype": "date",
                "numeric_scale": None,
                "numeric_precision": None,
                "char_size": None,
                "comment": None,
            },
        )

        self.assertEqual(
            rows[3].to_column_dict(omit_none=False),
            {
                "table_database": None,
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "table_owner": "root",
                "column": "struct_col",
                "column_index": 3,
                "dtype": "struct<struct_inner_col:string>",
                "numeric_scale": None,
                "numeric_precision": None,
                "char_size": None,
                "comment": None,
            },
        )

    def test_parse_relation_with_integer_owner(self):
        self.maxDiff = None
        rel_type = SparkRelation.get_relation_type.Table

        relation = SparkRelation.create(
            schema="default_schema", identifier="mytable", type=rel_type
        )
        assert relation.database is None

        # Mimics the output of Spark with a DESCRIBE TABLE EXTENDED
        plain_rows = [
            ("col1", "decimal(22,0)", None),
            ("# Detailed Table Information", None, None),
            ("Owner", 1234, None),
        ]

        input_cols = [Row(keys=["col_name", "data_type", "comment"], values=r) for r in plain_rows]

        rows = SparkAdapter(self.target_http, get_context("spawn")).parse_describe_extended(
            relation, input_cols
        )

        self.assertEqual(rows[0].to_column_dict().get("table_owner"), "1234")

    def test_parse_relation_with_statistics(self):
        self.maxDiff = None
        rel_type = SparkRelation.get_relation_type.Table

        relation = SparkRelation.create(
            schema="default_schema", identifier="mytable", type=rel_type
        )
        assert relation.database is None

        # Mimics the output of Spark with a DESCRIBE TABLE EXTENDED
        plain_rows = [
            ("col1", "decimal(22,0)", None),
            ("# Partition Information", "data_type", None),
            (None, None, None),
            ("# Detailed Table Information", None, None),
            ("Database", None, None),
            ("Owner", "root", None),
            ("Created Time", "Wed Feb 04 18:15:00 UTC 1815", None),
            ("Last Access", "Wed May 20 19:25:00 UTC 1925", None),
            ("Statistics", "1109049927 bytes, 14093476 rows", None),
            ("Type", "MANAGED", None),
            ("Provider", "delta", None),
            ("Location", "/mnt/vo", None),
            ("Serde Library", "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe", None),
            ("InputFormat", "org.apache.hadoop.mapred.SequenceFileInputFormat", None),
            ("OutputFormat", "org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat", None),
            ("Partition Provider", "Catalog", None),
        ]

        input_cols = [Row(keys=["col_name", "data_type", "comment"], values=r) for r in plain_rows]

        rows = SparkAdapter(self.target_http, get_context("spawn")).parse_describe_extended(
            relation, input_cols
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(
            rows[0].to_column_dict(omit_none=False),
            {
                "table_database": None,
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "table_owner": "root",
                "column": "col1",
                "column_index": 0,
                "dtype": "decimal(22,0)",
                "numeric_scale": None,
                "numeric_precision": None,
                "char_size": None,
                "comment": None,
                "stats:bytes:description": "",
                "stats:bytes:include": True,
                "stats:bytes:label": "bytes",
                "stats:bytes:value": 1109049927,
                "stats:rows:description": "",
                "stats:rows:include": True,
                "stats:rows:label": "rows",
                "stats:rows:value": 14093476,
            },
        )

    def test_relation_with_database(self):
        adapter = SparkAdapter(self.target_http, get_context("spawn"))
        # fine
        adapter.Relation.create(schema="different", identifier="table")
        # CCCS: database is now allowed on SparkRelation (cross-database/catalog support)
        relation = adapter.Relation.create(
            database="something", schema="different", identifier="table"
        )
        self.assertEqual(relation.database, "something")

    def test_profile_with_database(self):
        profile = {
            "outputs": {
                "test": {
                    "type": "spark",
                    "method": "http",
                    # not allowed
                    "database": "analytics2",
                    "schema": "analytics",
                    "host": "myorg.sparkhost.com",
                    "port": 443,
                    "token": "abc123",
                    "organization": "0123456789",
                    "cluster": "01234-23423-coffeetime",
                }
            },
            "target": "test",
        }
        with self.assertRaises(DbtRuntimeError):
            config_from_parts_or_dicts(self.base_project_cfg, profile)

    def test_profile_with_cluster_and_sql_endpoint(self):
        profile = {
            "outputs": {
                "test": {
                    "type": "spark",
                    "method": "odbc",
                    "schema": "analytics",
                    "host": "myorg.sparkhost.com",
                    "port": 443,
                    "token": "abc123",
                    "organization": "0123456789",
                    "cluster": "01234-23423-coffeetime",
                    "endpoint": "0123412341234e",
                }
            },
            "target": "test",
        }
        with self.assertRaises(DbtRuntimeError):
            config_from_parts_or_dicts(self.base_project_cfg, profile)

    def test_parse_columns_from_information_with_table_type_and_delta_provider(self):
        self.maxDiff = None
        rel_type = SparkRelation.get_relation_type.Table

        # Mimics the output of Spark in the information column
        information = (
            "Database: default_schema\n"
            "Table: mytable\n"
            "Owner: root\n"
            "Created Time: Wed Feb 04 18:15:00 UTC 1815\n"
            "Last Access: Wed May 20 19:25:00 UTC 1925\n"
            "Created By: Spark 3.0.1\n"
            "Type: MANAGED\n"
            "Provider: delta\n"
            "Statistics: 123456789 bytes\n"
            "Location: /mnt/vo\n"
            "Serde Library: org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe\n"
            "InputFormat: org.apache.hadoop.mapred.SequenceFileInputFormat\n"
            "OutputFormat: org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat\n"
            "Partition Provider: Catalog\n"
            "Partition Columns: [`dt`]\n"
            "Schema: root\n"
            " |-- col1: decimal(22,0) (nullable = true)\n"
            " |-- col2: string (nullable = true)\n"
            " |-- dt: date (nullable = true)\n"
            " |-- struct_col: struct (nullable = true)\n"
            " |    |-- struct_inner_col: string (nullable = true)\n"
        )
        relation = SparkRelation.create(
            schema="default_schema", identifier="mytable", type=rel_type, information=information
        )

        columns = SparkAdapter(
            self.target_http, get_context("spawn")
        ).parse_columns_from_information(relation)
        self.assertEqual(len(columns), 4)
        self.assertEqual(
            columns[0].to_column_dict(omit_none=False),
            {
                "table_database": None,
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "table_owner": "root",
                "column": "col1",
                "column_index": 0,
                "dtype": "decimal(22,0)",
                "numeric_scale": None,
                "numeric_precision": None,
                "char_size": None,
                "comment": None,
                "stats:bytes:description": "",
                "stats:bytes:include": True,
                "stats:bytes:label": "bytes",
                "stats:bytes:value": 123456789,
            },
        )

        self.assertEqual(
            columns[3].to_column_dict(omit_none=False),
            {
                "table_database": None,
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "table_owner": "root",
                "column": "struct_col",
                "column_index": 3,
                "dtype": "struct",
                "numeric_scale": None,
                "numeric_precision": None,
                "char_size": None,
                "comment": None,
                "stats:bytes:description": "",
                "stats:bytes:include": True,
                "stats:bytes:label": "bytes",
                "stats:bytes:value": 123456789,
            },
        )

    def test_parse_columns_from_information_with_view_type(self):
        self.maxDiff = None
        rel_type = SparkRelation.get_relation_type.View
        information = (
            "Database: default_schema\n"
            "Table: myview\n"
            "Owner: root\n"
            "Created Time: Wed Feb 04 18:15:00 UTC 1815\n"
            "Last Access: UNKNOWN\n"
            "Created By: Spark 3.0.1\n"
            "Type: VIEW\n"
            "View Text: WITH base (\n"
            "    SELECT * FROM source_table\n"
            ")\n"
            "SELECT col1, col2, dt FROM base\n"
            "View Original Text: WITH base (\n"
            "    SELECT * FROM source_table\n"
            ")\n"
            "SELECT col1, col2, dt FROM base\n"
            "View Catalog and Namespace: spark_catalog.default\n"
            "View Query Output Columns: [col1, col2, dt]\n"
            "Table Properties: [view.query.out.col.1=col1, view.query.out.col.2=col2, "
            "transient_lastDdlTime=1618324324, view.query.out.col.3=dt, "
            "view.catalogAndNamespace.part.0=spark_catalog, "
            "view.catalogAndNamespace.part.1=default]\n"
            "Serde Library: org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe\n"
            "InputFormat: org.apache.hadoop.mapred.SequenceFileInputFormat\n"
            "OutputFormat: org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat\n"
            "Storage Properties: [serialization.format=1]\n"
            "Schema: root\n"
            " |-- col1: decimal(22,0) (nullable = true)\n"
            " |-- col2: string (nullable = true)\n"
            " |-- dt: date (nullable = true)\n"
            " |-- struct_col: struct (nullable = true)\n"
            " |    |-- struct_inner_col: string (nullable = true)\n"
        )
        relation = SparkRelation.create(
            schema="default_schema", identifier="myview", type=rel_type, information=information
        )

        columns = SparkAdapter(
            self.target_http, get_context("spawn")
        ).parse_columns_from_information(relation)
        self.assertEqual(len(columns), 4)
        self.assertEqual(
            columns[1].to_column_dict(omit_none=False),
            {
                "table_database": None,
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "table_owner": "root",
                "column": "col2",
                "column_index": 1,
                "dtype": "string",
                "numeric_scale": None,
                "numeric_precision": None,
                "char_size": None,
                "comment": None,
            },
        )

        self.assertEqual(
            columns[3].to_column_dict(omit_none=False),
            {
                "table_database": None,
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "table_owner": "root",
                "column": "struct_col",
                "column_index": 3,
                "dtype": "struct",
                "numeric_scale": None,
                "numeric_precision": None,
                "char_size": None,
                "comment": None,
            },
        )

    def test_parse_columns_from_information_with_table_type_and_parquet_provider(self):
        self.maxDiff = None
        rel_type = SparkRelation.get_relation_type.Table

        information = (
            "Database: default_schema\n"
            "Table: mytable\n"
            "Owner: root\n"
            "Created Time: Wed Feb 04 18:15:00 UTC 1815\n"
            "Last Access: Wed May 20 19:25:00 UTC 1925\n"
            "Created By: Spark 3.0.1\n"
            "Type: MANAGED\n"
            "Provider: parquet\n"
            "Statistics: 1234567890 bytes, 12345678 rows\n"
            "Location: /mnt/vo\n"
            "Serde Library: org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe\n"
            "InputFormat: org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat\n"
            "OutputFormat: org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat\n"
            "Schema: root\n"
            " |-- col1: decimal(22,0) (nullable = true)\n"
            " |-- col2: string (nullable = true)\n"
            " |-- dt: date (nullable = true)\n"
            " |-- struct_col: struct (nullable = true)\n"
            " |    |-- struct_inner_col: string (nullable = true)\n"
        )
        relation = SparkRelation.create(
            schema="default_schema", identifier="mytable", type=rel_type, information=information
        )

        columns = SparkAdapter(
            self.target_http, get_context("spawn")
        ).parse_columns_from_information(relation)
        self.assertEqual(len(columns), 4)

        self.assertEqual(
            columns[2].to_column_dict(omit_none=False),
            {
                "table_database": None,
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "table_owner": "root",
                "column": "dt",
                "column_index": 2,
                "dtype": "date",
                "numeric_scale": None,
                "numeric_precision": None,
                "char_size": None,
                "comment": None,
                "stats:bytes:description": "",
                "stats:bytes:include": True,
                "stats:bytes:label": "bytes",
                "stats:bytes:value": 1234567890,
                "stats:rows:description": "",
                "stats:rows:include": True,
                "stats:rows:label": "rows",
                "stats:rows:value": 12345678,
            },
        )

        self.assertEqual(
            columns[3].to_column_dict(omit_none=False),
            {
                "table_database": None,
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "table_owner": "root",
                "column": "struct_col",
                "column_index": 3,
                "dtype": "struct",
                "numeric_scale": None,
                "numeric_precision": None,
                "char_size": None,
                "comment": None,
                "stats:bytes:description": "",
                "stats:bytes:include": True,
                "stats:bytes:label": "bytes",
                "stats:bytes:value": 1234567890,
                "stats:rows:description": "",
                "stats:rows:include": True,
                "stats:rows:label": "rows",
                "stats:rows:value": 12345678,
            },
        )


class TestListRelationsWithoutCaching(unittest.TestCase):
    @pytest.fixture(autouse=True)
    def set_up_fixtures(self, target_http):
        self.target_http = target_http

    def _make_adapter(self, flags=None):
        with mock.patch.object(self.target_http, "flags", flags or {}, create=True):
            return SparkAdapter(self.target_http, get_context("spawn"))

    def _make_schema_relation(self, adapter, schema="analytics"):
        return adapter.Relation.create(schema=schema, identifier="").without_identifier()

    def test_unknown_error_is_raised(self):
        """An unexpected error from the metastore should propagate rather than
        silently returning an empty list, which would cause incremental models
        to recreate tables and lose data (issue #1289)."""
        adapter = self._make_adapter()
        schema_relation = self._make_schema_relation(adapter)

        with mock.patch.object(
            adapter,
            "execute_macro",
            side_effect=DbtRuntimeError("Connection to Hive Metastore failed"),
        ):
            with self.assertRaises(DbtRuntimeError):
                adapter.list_relations_without_caching(schema_relation)

    def test_schema_not_found_returns_empty(self):
        """When the schema/database genuinely does not exist (legacy Hive
        'Database not found' message), an empty list should be returned."""
        adapter = self._make_adapter()
        schema_relation = self._make_schema_relation(adapter, schema="nonexistent")

        with mock.patch.object(
            adapter,
            "execute_macro",
            side_effect=DbtRuntimeError("Database not found"),
        ):
            result = adapter.list_relations_without_caching(schema_relation)
            self.assertEqual(result, [])

    def test_fast_listing_dispatches_for_iceberg_catalog(self):
        adapter = self._make_adapter()
        schema_relation = self._make_schema_relation(adapter)

        self.assertTrue(adapter.behavior.iceberg_listing_fast)
        with mock.patch.object(adapter, "_catalog_is_iceberg_only", return_value=True):
            with mock.patch.object(
                adapter, "_list_relations_fast", return_value=[]
            ) as fast_listing:
                adapter.list_relations_without_caching(schema_relation)

        fast_listing.assert_called_once_with(schema_relation)

    def test_fast_listing_can_be_disabled(self):
        adapter = self._make_adapter({"iceberg_listing_fast": False})
        schema_relation = self._make_schema_relation(adapter)

        self.assertFalse(adapter.behavior.iceberg_listing_fast)
        with mock.patch.object(adapter, "_catalog_is_iceberg_only") as is_iceberg:
            with mock.patch.object(
                adapter, "_list_relations_v2_first", return_value=[]
            ) as regular_listing:
                with mock.patch.object(adapter, "_list_relations_fast") as fast_listing:
                    adapter.list_relations_without_caching(schema_relation)

        is_iceberg.assert_not_called()
        fast_listing.assert_not_called()
        regular_listing.assert_called_once()

    def test_fast_listing_respects_v2_behavior_flag(self):
        adapter = self._make_adapter(
            {"iceberg_listing_fast": True, "use_v2_relation_listing": False}
        )
        schema_relation = self._make_schema_relation(adapter)

        with mock.patch.object(
            adapter, "_catalog_is_iceberg_only", return_value=True
        ) as is_iceberg:
            with mock.patch.object(
                adapter, "_list_relations_v1_first", return_value=[]
            ) as regular_listing:
                with mock.patch.object(adapter, "_list_relations_fast") as fast_listing:
                    adapter.list_relations_without_caching(schema_relation)

        is_iceberg.assert_not_called()
        fast_listing.assert_not_called()
        regular_listing.assert_called_once()

    def test_old_environment_switch_does_not_disable_fast_listing(self):
        adapter = self._make_adapter({"iceberg_listing_fast": True})
        schema_relation = self._make_schema_relation(adapter)
        with mock.patch.dict("os.environ", {"ICEBERG_LISTING_FAST": "0"}):
            with mock.patch.object(adapter, "_catalog_is_iceberg_only", return_value=True):
                with mock.patch.object(
                    adapter, "_list_relations_fast", return_value=[]
                ) as fast_listing:
                    adapter.list_relations_without_caching(schema_relation)
        fast_listing.assert_called_once_with(schema_relation)

    def test_fast_listing_builds_tables_and_views_without_describe(self):
        adapter = self._make_adapter()
        schema_relation = adapter.Relation.create(
            database="iceberg_catalog", schema="analytics", identifier=""
        ).without_identifier()
        tables = [
            ("analytics", "iceberg_table", False),
            ("analytics", "shared_name", False),
            ("", "temporary_table", True),
        ]
        views = [
            ("analytics", "shared_name", False),
            ("analytics", "iceberg_view", False),
            ("", "iceberg_table", True),
            ("", "temporary_view", True),
        ]

        with mock.patch.object(
            adapter, "execute", side_effect=[(None, tables), (None, views)]
        ) as execute:
            relations = adapter._list_relations_fast(schema_relation)

        execute.assert_has_calls(
            [
                mock.call(f"show tables in {schema_relation} like '*'", fetch=True),
                mock.call(f"show views in {schema_relation} like '*'", fetch=True),
            ]
        )
        self.assertEqual(execute.call_count, 2)
        self.assertEqual(
            [relation.identifier for relation in relations],
            ["iceberg_table", "shared_name", "iceberg_view"],
        )
        self.assertEqual(
            [relation.type for relation in relations],
            [RelationType.Table, RelationType.View, RelationType.View],
        )
        self.assertTrue(relations[0].is_iceberg)
        self.assertFalse(relations[1].is_iceberg)
        self.assertTrue(all(relation.database == "iceberg_catalog" for relation in relations))
        self.assertTrue(all(relation.schema == "analytics" for relation in relations))

    def test_fast_listing_returns_empty_for_missing_schema(self):
        adapter = self._make_adapter()
        schema_relation = self._make_schema_relation(adapter)

        for not_found_msg in SCHEMA_NOT_FOUND_MESSAGES:
            with self.subTest(not_found_msg=not_found_msg):
                with mock.patch.object(
                    adapter, "execute", side_effect=DbtRuntimeError(not_found_msg)
                ):
                    self.assertEqual(adapter._list_relations_fast(schema_relation), [])

    def test_fast_listing_propagates_unexpected_error(self):
        adapter = self._make_adapter()
        schema_relation = self._make_schema_relation(adapter)

        with mock.patch.object(
            adapter, "execute", side_effect=DbtRuntimeError("Connection failed")
        ):
            with self.assertRaises(DbtRuntimeError):
                adapter._list_relations_fast(schema_relation)

    def test_describe_not_found_returns_empty_information(self):
        adapter = self._make_adapter()
        schema_relation = adapter.Relation.create(
            database="iceberg_catalog", schema="analytics", identifier=""
        ).without_identifier()

        with mock.patch.object(
            adapter,
            "execute_macro",
            side_effect=DbtRuntimeError("[TABLE_OR_VIEW_NOT_FOUND]"),
        ):
            result = adapter._get_relation_information_using_describe(
                ("analytics", "missing_table", False), schema_relation
            )

        self.assertEqual(result, ("analytics", "missing_table", ""))

    def test_describe_propagates_unexpected_error(self):
        adapter = self._make_adapter()
        schema_relation = self._make_schema_relation(adapter)

        with mock.patch.object(
            adapter, "execute_macro", side_effect=DbtRuntimeError("Connection failed")
        ):
            with self.assertRaises(DbtRuntimeError):
                adapter._get_relation_information_using_describe(
                    ("analytics", "existing_table", False), schema_relation
                )


@pytest.fixture
def session_listing_adapter(target_http):
    adapter = SparkAdapter(target_http, get_context("spawn"))
    with mock.patch.object(adapter.config.credentials, "method", SparkConnectionMethod.SESSION):
        yield adapter


def test_catalog_class_detection_is_cached(session_listing_adapter):
    spark = mock.Mock()
    spark.conf.get.return_value = "org.apache.iceberg.spark.SparkCatalog"
    with mock.patch(
        "dbt.adapters.spark.session.get_spark_session", return_value=spark
    ) as get_session:
        assert session_listing_adapter._catalog_is_iceberg_only("polaris")
        assert session_listing_adapter._catalog_is_iceberg_only("polaris")

    spark.conf.get.assert_called_once_with("spark.sql.catalog.polaris")
    get_session.assert_called_with(
        session_listing_adapter.config.credentials.server_side_parameters
    )


def test_catalog_class_detection_uses_current_catalog(session_listing_adapter):
    spark = mock.Mock()
    spark.catalog.currentCatalog.return_value = "polaris"
    spark.conf.get.return_value = "org.apache.iceberg.spark.SparkCatalog"
    with mock.patch("dbt.adapters.spark.session.get_spark_session", return_value=spark):
        assert session_listing_adapter._catalog_is_iceberg_only(None)
    spark.conf.get.assert_called_once_with("spark.sql.catalog.polaris")


def test_catalog_class_detection_uses_jvm_fallback(session_listing_adapter):
    spark = mock.Mock()
    spark.conf.get.side_effect = RuntimeError("Configuration not set")
    catalog = spark._jsparkSession.sessionState().catalogManager().catalog.return_value
    catalog.getClass().getName.return_value = "org.apache.iceberg.spark.SparkCatalog"
    with mock.patch("dbt.adapters.spark.session.get_spark_session", return_value=spark):
        assert session_listing_adapter._catalog_is_iceberg_only("polaris")


def test_unknown_catalog_class_is_not_cached(session_listing_adapter):
    spark = mock.Mock()
    spark.conf.get.side_effect = [
        RuntimeError("Configuration unavailable"),
        "org.apache.iceberg.spark.SparkCatalog",
    ]
    spark._jsparkSession.sessionState.side_effect = RuntimeError("Catalog unavailable")
    with mock.patch("dbt.adapters.spark.session.get_spark_session", return_value=spark):
        assert not session_listing_adapter._catalog_is_iceberg_only("polaris")
        assert session_listing_adapter._catalog_class_cache == {}
        assert session_listing_adapter._catalog_is_iceberg_only("polaris")


def test_catalog_class_cache_does_not_cross_sessions(session_listing_adapter):
    iceberg_session = mock.Mock()
    iceberg_session.conf.get.return_value = "org.apache.iceberg.spark.SparkCatalog"
    other_session = mock.Mock()
    other_session.conf.get.return_value = "org.apache.iceberg.spark.SparkSessionCatalog"
    with mock.patch(
        "dbt.adapters.spark.session.get_spark_session",
        side_effect=[iceberg_session, other_session, iceberg_session],
    ):
        assert session_listing_adapter._catalog_is_iceberg_only("polaris")
        assert not session_listing_adapter._catalog_is_iceberg_only("polaris")
        assert session_listing_adapter._catalog_is_iceberg_only("polaris")
    iceberg_session.conf.get.assert_called_once()
    other_session.conf.get.assert_called_once()


def test_catalog_class_cache_does_not_cross_adapters(session_listing_adapter):
    spark = mock.Mock()
    spark.conf.get.side_effect = [
        "org.apache.iceberg.spark.SparkCatalog",
        "org.apache.iceberg.spark.SparkSessionCatalog",
    ]
    other_adapter = SparkAdapter(session_listing_adapter.config, get_context("spawn"))
    with mock.patch("dbt.adapters.spark.session.get_spark_session", return_value=spark):
        assert session_listing_adapter._catalog_is_iceberg_only("polaris")
        assert not other_adapter._catalog_is_iceberg_only("polaris")


def test_catalog_detection_does_not_create_local_session_for_remote_connections(target_http):
    adapter = SparkAdapter(target_http, get_context("spawn"))
    with mock.patch("dbt.adapters.spark.session.get_spark_session") as get_session:
        assert not adapter._catalog_is_iceberg_only("polaris")
    get_session.assert_not_called()


def test_catalog_detection_session_failure_uses_existing_listing(session_listing_adapter):
    with mock.patch(
        "dbt.adapters.spark.session.get_spark_session",
        side_effect=RuntimeError("Session unavailable"),
    ):
        assert not session_listing_adapter._catalog_is_iceberg_only("polaris")
    assert session_listing_adapter._catalog_class_cache == {}


def test_fast_listing_preserves_namespace_identity(target_http):
    adapter = SparkAdapter(target_http, get_context("spawn"))
    schema_relation = adapter.Relation.create(
        database="polaris", schema="parent.child", identifier=""
    ).without_identifier()
    with mock.patch.object(
        adapter,
        "execute",
        side_effect=[
            (None, [("`parent`.`child`", "orders", False)]),
            (None, [("`parent`.`child`", "report", False)]),
        ],
    ):
        relations = adapter._list_relations_fast(schema_relation)
    assert [relation.schema for relation in relations] == ["parent.child", "parent.child"]


@pytest.mark.parametrize("statement", [0, 1])
@pytest.mark.parametrize("message", ["Permission denied", "[TABLE_OR_VIEW_NOT_FOUND]"])
def test_fast_listing_errors_propagate(target_http, statement, message):
    adapter = SparkAdapter(target_http, get_context("spawn"))
    schema_relation = adapter.Relation.create(schema="analytics").without_identifier()
    with mock.patch.object(
        adapter, "execute", side_effect=[(None, [])] * statement + [DbtRuntimeError(message)]
    ):
        with pytest.raises(DbtRuntimeError) as error:
            adapter._list_relations_fast(schema_relation)
    assert error.value.msg == message


@pytest.mark.parametrize("statement", [0, 1])
def test_fast_listing_missing_schema_returns_no_partial_results(target_http, statement):
    adapter = SparkAdapter(target_http, get_context("spawn"))
    schema_relation = adapter.Relation.create(schema="analytics").without_identifier()
    with mock.patch.object(
        adapter,
        "execute",
        side_effect=[(None, [("analytics", "orders", False)])] * statement
        + [DbtRuntimeError("[SCHEMA_NOT_FOUND]")],
    ):
        assert adapter._list_relations_fast(schema_relation) == []


@pytest.mark.parametrize("not_found_msg", SCHEMA_NOT_FOUND_MESSAGES)
def test_all_schema_not_found_messages_return_empty(not_found_msg, target_http):
    """Every message in SCHEMA_NOT_FOUND_MESSAGES should cause
    list_relations_without_caching to return [] rather than raise.
    This covers engine-specific variants such as Spark SQL's [SCHEMA_NOT_FOUND]."""
    adapter = SparkAdapter(target_http, get_context("spawn"))
    schema_relation = adapter.Relation.create(
        schema="nonexistent", identifier=""
    ).without_identifier()

    with mock.patch.object(
        adapter,
        "execute_macro",
        side_effect=DbtRuntimeError(not_found_msg),
    ):
        result = adapter.list_relations_without_caching(schema_relation)
        assert result == []


@pytest.mark.parametrize("not_found_msg", TABLE_OR_VIEW_NOT_FOUND_MESSAGES)
def test_all_table_or_view_not_found_messages_return_empty(not_found_msg, target_http):
    """Every message in TABLE_OR_VIEW_NOT_FOUND_MESSAGES should cause
    list_relations_without_caching to return [] rather than raise.
    This covers Databricks/Simba errors like [TABLE_OR_VIEW_NOT_FOUND] that
    surface when a schema has no matching tables."""
    adapter = SparkAdapter(target_http, get_context("spawn"))
    schema_relation = adapter.Relation.create(
        schema="nonexistent", identifier=""
    ).without_identifier()

    with mock.patch.object(
        adapter,
        "execute_macro",
        side_effect=DbtRuntimeError(not_found_msg),
    ):
        result = adapter.list_relations_without_caching(schema_relation)
        assert result == []


ICEBERG_V2_ERROR = "SHOW TABLE EXTENDED is not supported for v2 tables"


class TestListRelationsIcebergV2Fallback(unittest.TestCase):
    """Tests for the Iceberg v2 fallback path inside list_relations_without_caching.

    When the primary SHOW TABLE EXTENDED macro raises the v2-table error, the adapter
    falls back to a SHOW TABLES macro.  Unexpected errors from *that* fallback should
    propagate rather than be swallowed.
    """

    @pytest.fixture(autouse=True)
    def set_up_fixtures(self, target_http):
        self.target_http = target_http

    def _make_adapter(self):
        return SparkAdapter(self.target_http, get_context("spawn"))

    def _make_schema_relation(self, adapter, schema="analytics"):
        return adapter.Relation.create(schema=schema, identifier="").without_identifier()

    def test_iceberg_v2_fallback_returns_relations(self):
        """When the primary macro raises the v2-table error, the fallback SHOW TABLES
        macro is called and its results are returned successfully."""
        adapter = self._make_adapter()
        schema_relation = self._make_schema_relation(adapter)

        fallback_rows = mock.MagicMock()
        fallback_rows.__iter__ = mock.Mock(return_value=iter([]))

        def execute_macro_side_effect(macro_name, *args, **kwargs):
            if macro_name == LIST_RELATIONS_MACRO_NAME:
                raise DbtRuntimeError(ICEBERG_V2_ERROR)
            return fallback_rows

        with mock.patch.object(adapter, "execute_macro", side_effect=execute_macro_side_effect):
            with mock.patch.object(
                adapter, "_build_spark_relation_list", return_value=[]
            ) as mock_build:
                result = adapter.list_relations_without_caching(schema_relation)
                self.assertEqual(result, [])
                mock_build.assert_called_once()

    def test_iceberg_v2_fallback_error_propagates(self):
        """If the fallback SHOW TABLES macro itself raises an unexpected error, that
        error should propagate rather than being silently swallowed."""
        adapter = self._make_adapter()
        schema_relation = self._make_schema_relation(adapter)

        def execute_macro_side_effect(macro_name, *args, **kwargs):
            if macro_name == LIST_RELATIONS_MACRO_NAME:
                raise DbtRuntimeError(ICEBERG_V2_ERROR)
            raise DbtRuntimeError("Unexpected fallback failure")

        with mock.patch.object(adapter, "execute_macro", side_effect=execute_macro_side_effect):
            with self.assertRaises(DbtRuntimeError):
                adapter.list_relations_without_caching(schema_relation)


class TestGetColumnsForCatalogIcebergFallback(unittest.TestCase):
    """Tests for _get_columns_for_catalog falling back to get_columns_in_relation
    when the information string (from the DESCRIBE EXTENDED / Iceberg v2 path)
    does not contain column definitions in the regex-parseable format."""

    @pytest.fixture(autouse=True)
    def set_up_fixtures(self, target_http):
        self.target_http = target_http

    def _make_adapter(self):
        return SparkAdapter(self.target_http, get_context("spawn"))

    def test_falls_back_to_get_columns_in_relation(self):
        """When parse_columns_from_information returns nothing,
        _get_columns_for_catalog should fall back to get_columns_in_relation."""
        adapter = self._make_adapter()

        # Information string with no ' |-- col: type (nullable = ...)' lines,
        # so parse_columns_from_information will return an empty list.
        information = "id: int\n" "name: string\n" "Provider: iceberg\n" "Owner: root\n"
        relation = SparkRelation.create(
            schema="default",
            identifier="orders",
            type=SparkRelation.get_relation_type.Table,
            information=information,
        )

        fallback_columns = [
            SparkColumn(
                table_database=None,
                table_schema="default",
                table_name="orders",
                table_type=SparkRelation.get_relation_type.Table,
                column_index=0,
                table_owner="root",
                column="id",
                dtype="int",
            ),
            SparkColumn(
                table_database=None,
                table_schema="default",
                table_name="orders",
                table_type=SparkRelation.get_relation_type.Table,
                column_index=1,
                table_owner="root",
                column="name",
                dtype="string",
            ),
        ]

        with mock.patch.object(
            adapter, "get_columns_in_relation", return_value=fallback_columns
        ) as mock_get_cols:
            result = list(adapter._get_columns_for_catalog(relation))

        mock_get_cols.assert_called_once_with(relation)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["column_name"], "id")
        self.assertEqual(result[0]["column_type"], "int")
        self.assertEqual(result[1]["column_name"], "name")
        self.assertEqual(result[1]["column_type"], "string")

    def test_does_not_fall_back_when_information_has_columns(self):
        """When parse_columns_from_information succeeds (SHOW TABLE EXTENDED path),
        get_columns_in_relation should NOT be called."""
        adapter = self._make_adapter()

        # Standard information string with the regex-parseable format
        information = (
            "Owner: root\n"
            "Schema: root\n"
            " |-- id: int (nullable = true)\n"
            " |-- name: string (nullable = true)\n"
        )
        relation = SparkRelation.create(
            schema="default",
            identifier="orders",
            type=SparkRelation.get_relation_type.Table,
            information=information,
        )

        with mock.patch.object(adapter, "get_columns_in_relation") as mock_get_cols:
            result = list(adapter._get_columns_for_catalog(relation))

        mock_get_cols.assert_not_called()
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["column_name"], "id")
        self.assertEqual(result[1]["column_name"], "name")

    def test_fallback_swallows_runtime_error(self):
        """If the fallback get_columns_in_relation raises DbtRuntimeError"""
        adapter = self._make_adapter()

        relation = SparkRelation.create(
            schema="default",
            identifier="orders",
            type=SparkRelation.get_relation_type.Table,
            information="Provider: iceberg\n",
        )

        with mock.patch.object(
            adapter,
            "get_columns_in_relation",
            side_effect=DbtRuntimeError("describe failed"),
        ):
            result = list(adapter._get_columns_for_catalog(relation))

        self.assertEqual(result, [])
