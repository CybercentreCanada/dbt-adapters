import pytest
import yaml

from dbt.tests.util import relation_from_name, run_dbt, run_dbt_and_capture


_VALID_SQL = """
select
    'first' as label,
    cast(1 as bigint) as id,
    true as active,
    array('one', 'two') as tags,
    cast(12.34 as decimal(10, 2)) as amount,
    cast('2026-01-01' as date) as event_date,
    cast('2026-01-01 12:00:00' as timestamp) as event_time
"""

_COLUMNS = [
    {"name": "id", "data_type": "bigint"},
    {"name": "label", "data_type": "string"},
    {"name": "active", "data_type": "boolean"},
    {"name": "tags", "data_type": "array<string>"},
    {"name": "amount", "data_type": "decimal(10,2)"},
    {"name": "event_date", "data_type": "date"},
    {"name": "event_time", "data_type": "timestamp"},
]


class BaseSQLContractSession:
    materialization = "table"

    @pytest.fixture(scope="class")
    def project_config_update(self, local_session_profile):
        return {
            "models": {
                "+materialized": self.materialization,
                "+contract": {"enforced": True},
                "+on_schema_change": "fail",
                "+incremental_strategy": "merge",
                "+unique_key": "id",
            }
        }

    @pytest.fixture(scope="class")
    def models(self, local_session_profile):
        return {
            "valid.sql": _VALID_SQL,
            "wrong_name.sql": _VALID_SQL.replace("as id,", "as wrong_id,"),
            "wrong_type.sql": _VALID_SQL.replace("cast(1 as bigint)", "'not-an-integer'"),
            "schema.yml": yaml.safe_dump(
                {
                    "version": 2,
                    "models": [
                        {"name": name, "columns": _COLUMNS}
                        for name in ["valid", "wrong_name", "wrong_type"]
                    ],
                }
            ),
        }

    def test_sql_contract_accepts_declared_types_and_preserves_rows(self, project):
        assert len(run_dbt(["run", "--select", "valid"])) == 1
        relation = relation_from_name(project.adapter, "valid")
        with project.adapter.connection_named("contract_columns"):
            columns = project.adapter.get_columns_in_relation(relation)
        assert {column.name: column.dtype for column in columns} == {
            "id": "bigint",
            "label": "string",
            "active": "boolean",
            "tags": "array<string>",
            "amount": "decimal(10,2)",
            "event_date": "date",
            "event_time": "timestamp",
        }
        if self.materialization != "view":
            assert [column.name for column in columns] == [
                "id",
                "label",
                "active",
                "tags",
                "amount",
                "event_date",
                "event_time",
            ]
        if self.materialization == "incremental":
            assert len(run_dbt(["run", "--select", "valid"])) == 1
        rows = project.run_sql(
            f"select id, label, active, size(tags), cast(amount as string), "
            f"cast(event_date as string), cast(event_time as string) from {relation}",
            fetch="all",
        )
        assert [tuple(row) for row in rows] == [
            (1, "first", True, 2, "12.34", "2026-01-01", "2026-01-01 12:00:00")
        ]

    @pytest.mark.parametrize(
        "model, diagnostic",
        [
            ("wrong_name", "missing in contract"),
            ("wrong_type", "data type mismatch"),
        ],
    )
    def test_sql_contract_rejects_mismatch_before_publication(self, project, model, diagnostic):
        results, output = run_dbt_and_capture(["run", "--select", model], expect_pass=False)
        assert len(results) == 1
        assert diagnostic in output.lower()
        assert "id" in output.lower()
        with project.adapter.connection_named("rejected_contract"):
            schema = project.adapter.Relation.create(
                database=project.database, schema=project.test_schema
            )
            relations = project.adapter.list_relations_without_caching(schema)
        assert model not in {relation.identifier for relation in relations}


class TestTableSQLContractSession(BaseSQLContractSession):
    pass


class TestIncrementalSQLContractSession(BaseSQLContractSession):
    materialization = "incremental"


class TestViewSQLContractSession(BaseSQLContractSession):
    materialization = "view"
