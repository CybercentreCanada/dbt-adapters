import pytest

from dbt.adapters.spark.session import get_spark_session
from dbt.tests.util import run_dbt


class TestIcebergViewRelations:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"require_location_root": False}}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "orders.sql": (
                "{{ config(materialized='table', file_format='iceberg') }}" " select 1 as id"
            ),
            "report.sql": ("{{ config(materialized='view') }} select * from {{ ref('orders') }}"),
        }

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {
            "orders_snapshot.sql": """
{% snapshot orders_snapshot %}
{{ config(
    target_schema=target.schema,
    unique_key='id',
    strategy='check',
    check_cols=['id'],
    file_format='iceberg'
) }}
select * from {{ ref('orders') }}
{% endsnapshot %}
"""
        }

    @pytest.fixture(autouse=True)
    def require_iceberg_session(self, project):
        if not project.adapter._catalog_is_iceberg_only(project.database):
            pytest.skip("Requires a session profile configured with an Iceberg SparkCatalog")

    def test_view_listing_and_snapshot_lifecycle(self, project):
        run_dbt(["run"])
        run_dbt(["run", "--select", "report"])
        run_dbt(["snapshot"])
        run_dbt(["snapshot"])

        adapter = project.adapter
        schema_relation = adapter.Relation.create(
            database=project.database, schema=project.test_schema
        )
        with adapter.connection_named("iceberg_view_listing"):
            adapter.execute("create or replace temporary view orders as select 2 as id")
            try:
                relations = adapter.list_relations_without_caching(schema_relation)
                by_name = {relation.identifier: relation for relation in relations}
                assert by_name["orders"].is_table
                assert by_name["orders"].is_iceberg
                assert by_name["report"].is_view
                assert all(relation.schema == project.test_schema for relation in relations)
                assert "orders_snapshot__dbt_tmp" not in by_name

                _, views = adapter.execute(f"show views in {schema_relation}", fetch=True)
                assert not any(row[1] == "orders_snapshot__dbt_tmp" for row in views)
                snapshot = schema_relation.incorporate(path={"identifier": "orders_snapshot"})
                _, rows = adapter.execute(f"select count(*) from {snapshot}", fetch=True)
                assert rows[0][0] == 1

                spark = get_spark_session(adapter.config.credentials.server_side_parameters)
                other_session = spark.newSession()
                report = schema_relation.incorporate(path={"identifier": "report"})
                assert other_session.sql(f"select id from {report}").collect()[0][0] == 1
                other_views = other_session.sql(f"show views in {schema_relation}").collect()
                assert not any(row[1] == "orders" and row[2] for row in other_views)
            finally:
                adapter.execute("drop view if exists orders")
