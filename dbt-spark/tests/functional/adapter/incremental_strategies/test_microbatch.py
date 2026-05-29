import pytest

from dbt.tests.adapter.incremental.test_incremental_microbatch import (
    BaseMicrobatch,
)
from dbt.tests.util import run_dbt_and_capture

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
