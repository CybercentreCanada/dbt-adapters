import pytest
import yaml

from dbt.adapters.spark.session import get_spark_session
from dbt_common.exceptions import DbtRuntimeError
from dbt.tests.util import relation_from_name, run_dbt, run_dbt_and_capture, write_file


class TestPersistDocsSession:
    @pytest.fixture(scope="class")
    def project_config_update(self, local_session_profile):
        return {}

    @pytest.fixture(scope="class")
    def models(self, local_session_profile):
        return {
            "accounts.sql": """
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='Account_ID',
    persist_docs={'relation': true, 'columns': true}
) }}
select cast(1 as bigint) as Account_ID, cast('first' as string) as label
"""
        }

    def test_comments_are_created_updated_cleared_and_diffed(self, project):
        relation = None
        for description in ["Account owner's records", "Updated owner's records", ""]:
            columns = {
                "account_id": {"name": "account_id", "description": description},
                "label": {"name": "label", "description": ""},
            }
            schema = {
                "version": 2,
                "models": [
                    {
                        "name": "accounts",
                        "description": description,
                        "columns": list(columns.values()),
                    }
                ],
            }
            write_file(yaml.safe_dump(schema), project.project_root, "models", "schema.yml")
            assert len(run_dbt(["run"])) == 1
            relation = relation_from_name(project.adapter, "accounts")
            with project.adapter.connection_named("persisted_comments"):
                actual = project.adapter.get_columns_in_relation(relation)
                assert [(column.name, column.comment or "") for column in actual] == [
                    ("Account_ID", description),
                    ("label", ""),
                ]
                _, metadata = project.adapter.execute(f"describe extended {relation}", fetch=True)
                table_comments = [row[1] for row in metadata.rows if row[0] == "Comment"]
                assert table_comments == ([description] if description else [""])
                assert project.adapter.get_persist_docs_diff(
                    relation, description, columns, True, True
                ) == {"relation_comment": None, "columns": {}}
        assert project.run_sql(f"select count(*) from {relation}", fetch="one")[0] == 1


class BasePartitionSyncSession:
    enabled = True
    raises = True
    initial = ["region"]
    desired = ["region"]
    drift = False

    @pytest.fixture(scope="class")
    def project_config_update(self, local_session_profile):
        return {
            "flags": {
                "check_partition_sync": self.enabled,
                "check_partition_sync_raises": self.raises,
            }
        }

    @pytest.fixture(scope="class")
    def models(self, local_session_profile):
        return {"events.sql": self.model_sql(self.initial)}

    @staticmethod
    def model_sql(partitions):
        return (
            "{{ config(materialized='incremental', incremental_strategy='append', "
            f"partition_by={partitions!r}) }}}}\n"
            "select cast(1 as bigint) as id, 'west' as region, "
            "cast('2026-01-01 12:00:00' as timestamp) as event_time"
        )

    def test_partition_policy_uses_live_metadata(self, project):
        assert len(run_dbt(["run"])) == 1
        relation = relation_from_name(project.adapter, "events")
        original_ddl = project.run_sql(f"show create table {relation}", fetch="one")[0]
        original_partition = next(
            line for line in original_ddl.splitlines() if line.startswith("PARTITIONED BY")
        )
        with project.adapter.connection_named("partition_columns"):
            columns = project.adapter.get_columns_in_relation(relation)
        assert [column.name for column in columns] == ["id", "region", "event_time"]

        write_file(self.model_sql(self.desired), project.project_root, "models", "events.sql")
        blocked = self.enabled and self.raises and self.drift
        results, output = run_dbt_and_capture(["run"], expect_pass=not blocked)
        assert len(results) == 1
        assert ("Partition drift detected" in output) == (self.enabled and self.drift)
        assert project.run_sql(f"select count(*) from {relation}", fetch="one")[0] == (
            1 if blocked else 2
        )
        current_ddl = project.run_sql(f"show create table {relation}", fetch="one")[0]
        assert (
            next(line for line in current_ddl.splitlines() if line.startswith("PARTITIONED BY"))
            == original_partition
        )


class TestPartitionColumnSession(BasePartitionSyncSession):
    pass


class TestPartitionTransformsSession(BasePartitionSyncSession):
    initial = ["days(event_time)", "bucket(8, id)"]
    desired = ["days(event_time)", "bucket(8, id)"]


class TestPartitionEquivalentQuotingSession(BasePartitionSyncSession):
    desired = ["`region`"]


class TestPartitionEquivalentTransformSpacingSession(BasePartitionSyncSession):
    initial = ["days(event_time)", "bucket(8, id)"]
    desired = ["days( `event_time` )", "bucket(8,   `id`)"]


class TestPartitionCaseSensitiveSession(BasePartitionSyncSession):
    def test_partition_policy_uses_live_metadata(self, project):
        assert len(run_dbt(["run"])) == 1
        adapter = project.adapter
        relation = relation_from_name(adapter, "events")
        spark = get_spark_session(adapter.config.credentials.server_side_parameters)
        original = spark.conf.get("spark.sql.caseSensitive")
        try:
            with adapter.connection_named("partition_case_sensitivity"):
                spark.conf.set("spark.sql.caseSensitive", "false")
                adapter.check_partition_sync(relation, "iceberg", ["REGION"])
                spark.conf.set("spark.sql.caseSensitive", "true")
                with pytest.raises(DbtRuntimeError, match="Partition drift detected"):
                    adapter.check_partition_sync(relation, "iceberg", ["REGION"])
                adapter.check_partition_sync(relation, "iceberg", ["`region`"])
        finally:
            spark.conf.set("spark.sql.caseSensitive", original)


class TestPartitionDriftWarningSession(BasePartitionSyncSession):
    raises = False
    desired = ["id"]
    drift = True


class TestPartitionDriftErrorSession(BasePartitionSyncSession):
    desired = None
    drift = True


class TestPartitionCheckDisabledSession(BasePartitionSyncSession):
    enabled = False
    desired = ["id"]
    drift = True
