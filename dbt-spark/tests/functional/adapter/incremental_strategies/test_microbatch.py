import pytest

from dbt.tests.adapter.incremental.test_incremental_microbatch import (
    BaseMicrobatch,
    _input_model_sql,
)
from dbt.tests.util import run_dbt_and_capture, write_file

try:
    from dbt.tests.util import patch_microbatch_end_time
except ImportError:
    from freezegun import freeze_time as patch_microbatch_end_time

# No requirement for a unique_id for spark microbatch!
# dbt-spark restricts the microbatch incremental strategy to file_format='iceberg'
# because Iceberg's snapshot-isolated INSERT OVERWRITE is required for safe
# parallel batch execution.
_microbatch_model_no_unique_id_sql = """
{{ config(materialized='incremental', incremental_strategy='microbatch', event_time='event_time', batch_size='day', begin=modules.datetime.datetime(2020, 1, 1, 0, 0, 0), partition_by=['date_day'], file_format='iceberg') }}
select *, cast(event_time as date) as date_day
from {{ ref('input_model') }}
"""

# Negative-case model: microbatch + non-iceberg file_format must fail at compile time.
_microbatch_model_parquet_invalid_sql = """
{{ config(materialized='incremental', incremental_strategy='microbatch', event_time='event_time', batch_size='day', begin=modules.datetime.datetime(2020, 1, 1, 0, 0, 0), partition_by=['date_day'], file_format='parquet') }}
select *, cast(event_time as date) as date_day
from {{ ref('input_model') }}
"""


@pytest.mark.skip_profile(
    "databricks_http_cluster", "databricks_sql_endpoint", "spark_session", "spark_http_odbc"
)
class TestMicrobatch(BaseMicrobatch):
    @pytest.fixture(scope="class")
    def microbatch_model_sql(self) -> str:
        return _microbatch_model_no_unique_id_sql


@pytest.mark.skip_profile(
    "databricks_http_cluster", "databricks_sql_endpoint", "spark_session", "spark_http_odbc"
)
class TestMicrobatchNonIcebergRejected:
    """
    Verifies that dbt-spark rejects `incremental_strategy='microbatch'` when
    `file_format` is not 'iceberg'. The compiler error should mention iceberg
    so users can self-correct.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "input_model.sql": (
                "{{ config(materialized='table') }}\n"
                "select 1 as id, "
                "cast('2020-01-01 00:00:00' as timestamp) as event_time"
            ),
            "microbatch_invalid.sql": _microbatch_model_parquet_invalid_sql,
        }

    def test_microbatch_requires_iceberg(self, project):
        _, log_output = run_dbt_and_capture(["run"], expect_pass=False)
        assert "microbatch" in log_output.lower()
        assert "iceberg" in log_output.lower()


def _microbatch_docs_model_sql(prop_value: str) -> str:
    return (
        "{{ config(materialized='incremental', incremental_strategy='microbatch', "
        "event_time='event_time', batch_size='day', "
        "begin=modules.datetime.datetime(2020, 1, 1, 0, 0, 0), partition_by=['date_day'], "
        "file_format='iceberg', concurrent_batches=true, "
        f"tblproperties={{'dbt.test.prop': '{prop_value}'}}) }}}}\n"
        "select id, event_time, cast(event_time as date) as date_day\n"
        "from {{ ref('input_model') }}\n"
    )


def _microbatch_docs_schema_yml(table_desc: str, column_desc: str) -> str:
    return f"""
version: 2
models:
  - name: microbatch_docs
    description: "{table_desc}"
    config:
      persist_docs:
        relation: true
        columns: true
    columns:
      - name: id
        description: "{column_desc}"
"""


@pytest.mark.skip_profile(
    "databricks_http_cluster", "databricks_sql_endpoint", "spark_session", "spark_http_odbc"
)
class TestMicrobatchConcurrentMetadataSync:
    """Concurrent microbatch batches must sync comments/tblproperties once, and only on drift."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "input_model.sql": _input_model_sql,
            "microbatch_docs.sql": _microbatch_docs_model_sql("v1"),
            "schema.yml": _microbatch_docs_schema_yml("table v1", "id v1"),
        }

    def _run(self):
        with patch_microbatch_end_time("2020-01-03 13:57:00"):
            _, log_output = run_dbt_and_capture(["--debug", "run", "--threads", "4"])
        return log_output

    def _describe(self, project):
        rows = project.run_sql(
            f"describe extended {project.test_schema}.microbatch_docs", fetch="all"
        )
        return {str(r[0]).strip(): r for r in rows}

    def test_metadata_synced_once_on_drift(self, project):
        self._run()

        write_file(
            _microbatch_docs_model_sql("v2"), project.project_root, "models", "microbatch_docs.sql"
        )
        write_file(
            _microbatch_docs_schema_yml("table v2", "id v2"),
            project.project_root,
            "models",
            "schema.yml",
        )
        log_output = self._run()
        assert log_output.count("Updating table comment on") == 1
        assert log_output.count("Tblproperties drift on") == 1

        described = self._describe(project)
        assert described["Comment"][1] == "table v2"
        assert described["id"][2] == "id v2"

        log_output = self._run()
        assert "Updating table comment on" not in log_output
        assert "Tblproperties drift on" not in log_output
