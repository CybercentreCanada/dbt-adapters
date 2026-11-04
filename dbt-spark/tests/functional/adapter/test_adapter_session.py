from uuid import uuid4

import pytest

from dbt.adapters.spark.connections import SparkConnectionMethod
from dbt_common.exceptions import DbtRuntimeError
from dbt.tests.util import run_dbt


class TestAdapterSession:
    @pytest.fixture(scope="class")
    def project_config_update(self, local_session_profile):
        return {}

    @pytest.fixture(scope="class")
    def models(self, local_session_profile):
        return {
            "orders.sql": """
{{ config(materialized='table') }}
select cast(1 as bigint) as id, cast('first' as string) as label
union all
select cast(2 as bigint), cast('second' as string)
""",
            "report.sql": """
{{ config(materialized='view') }}
select * from {{ ref('orders') }}
""",
        }

    @pytest.fixture(scope="class")
    def session_project(self, project):
        assert len(run_dbt(["run"])) == 2
        return project

    def test_connection_uses_spark_session_and_configured_settings(self, session_project):
        adapter = session_project.adapter
        assert adapter.type() == "spark"
        assert adapter.config.credentials.method == SparkConnectionMethod.SESSION
        with adapter.connection_named("session_settings"):
            _, result = adapter.execute(
                "select current_catalog() as catalog, current_timezone() as timezone",
                fetch=True,
            )
            connection = adapter.connections.get_thread_connection()
            assert connection.state == "open"
            assert connection.handle is not None
            assert [tuple(row) for row in result.rows] == [("iceberg", "UTC")]

    def test_execute_returns_named_columns_and_rows(self, session_project):
        adapter = session_project.adapter
        target = f"iceberg.{session_project.test_schema}.orders"
        with adapter.connection_named("session_query"):
            response, result = adapter.execute(
                f"select id, label from {target} order by id", fetch=True
            )
        assert response._message == "OK"
        assert result.column_names == ("id", "label")
        assert [tuple(row) for row in result.rows] == [(1, "first"), (2, "second")]

    def test_query_errors_propagate_and_next_query_succeeds(self, session_project):
        adapter = session_project.adapter
        missing = f"iceberg.{session_project.test_schema}.missing_{uuid4().hex}"
        with pytest.raises(DbtRuntimeError, match="TABLE_OR_VIEW_NOT_FOUND"):
            with adapter.connection_named("session_error"):
                adapter.execute(f"select * from {missing}", fetch=True)
        with adapter.connection_named("session_recovery"):
            _, result = adapter.execute("select 42 as answer", fetch=True)
        assert [tuple(row) for row in result.rows] == [(42,)]

    def test_get_columns_returns_live_table_metadata(self, session_project):
        adapter = session_project.adapter
        relation = adapter.Relation.create(
            database="iceberg",
            schema=session_project.test_schema,
            identifier="orders",
        )
        with adapter.connection_named("session_columns"):
            columns = adapter.get_columns_in_relation(relation)
        assert [(column.name, column.dtype) for column in columns] == [
            ("id", "bigint"),
            ("label", "string"),
        ]

    def test_listing_returns_persistent_tables_and_views_only(self, session_project):
        adapter = session_project.adapter
        schema = adapter.Relation.create(database="iceberg", schema=session_project.test_schema)
        temporary = f"session_temp_{uuid4().hex}"
        with adapter.connection_named("session_listing"):
            adapter.execute(f"create temporary view {temporary} as select 3 as id")
            try:
                relations = adapter.list_relations_without_caching(schema)
                by_name = {relation.identifier: relation for relation in relations}
                assert set(by_name) == {"orders", "report"}
                assert by_name["orders"].is_table
                assert by_name["orders"].is_iceberg
                assert by_name["report"].is_view
                assert not by_name["report"].is_iceberg
                assert all(
                    relation.database == "iceberg"
                    and relation.schema == session_project.test_schema
                    for relation in relations
                )
            finally:
                adapter.execute(f"drop view {temporary}")
