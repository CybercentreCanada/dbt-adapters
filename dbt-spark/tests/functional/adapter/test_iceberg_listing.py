import os

import pytest

from dbt.adapters.spark.connections import SparkConnectionMethod


# CCCS opt in only with a session catalog supporting persistent Iceberg views.
pytestmark = pytest.mark.skipif(
    os.getenv("DBT_RUN_ICEBERG_LISTING_TESTS") != "1",
    reason="Requires an Iceberg SparkCatalog with persistent-view support",
)


class TestIcebergListing:
    def test_fast_listing_matches_regular_listing(self, project):
        adapter = project.adapter
        if adapter.config.credentials.method != SparkConnectionMethod.SESSION:
            pytest.skip("Requires a session connection")
        schema = adapter.Relation.create(database=project.database, schema=project.test_schema)
        with adapter.connection_named("iceberg_listing"):
            if not adapter._catalog_is_iceberg_only(schema.database):
                pytest.skip("Requires an Iceberg SparkCatalog")
            table = schema.incorporate(path={"identifier": "listing_orders"})
            view = schema.incorporate(path={"identifier": "listing_report"})
            adapter.execute(f"create table {table} using iceberg as select 1 as id")
            try:
                adapter.execute(f"create view {view} as select * from {table}")
                try:
                    regular = adapter._list_relations_v2_first({"schema_relation": schema}, schema)
                    fast = adapter.list_relations_without_caching(schema)

                    def identity(relation):
                        return (
                            relation.database,
                            relation.schema,
                            relation.identifier,
                            relation.type,
                            relation.is_iceberg,
                        )

                    assert {identity(r) for r in fast} == {identity(r) for r in regular}
                    by_name = {r.identifier: r for r in fast}
                    assert by_name["listing_orders"].is_table
                    assert by_name["listing_orders"].is_iceberg
                    assert by_name["listing_report"].is_view
                    assert not by_name["listing_report"].is_iceberg

                    adapter.execute("create temporary view listing_orders as select 2 as id")
                    try:
                        after = adapter.list_relations_without_caching(schema)
                        assert {identity(r) for r in after} == {identity(r) for r in fast}
                    finally:
                        adapter.execute("drop view listing_orders")
                finally:
                    adapter.execute(f"drop view {view}")
            finally:
                adapter.execute(f"drop table {table}")
