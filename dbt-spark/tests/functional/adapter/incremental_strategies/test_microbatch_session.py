import pytest

from dbt.artifacts.schemas.results import RunStatus
from dbt.tests.util import relation_from_name, run_dbt


_input_model_sql = """
{{ config(materialized='table', event_time='event_time') }}
select 1 as id, cast('2020-01-01 00:15:00' as timestamp) as event_time
union all
select 2 as id, cast('2020-01-01 00:45:00' as timestamp) as event_time
union all
select 3 as id, cast('2020-01-01 01:10:00' as timestamp) as event_time
union all
select 4 as id, cast('2020-01-01 01:50:00' as timestamp) as event_time
union all
select 5 as id, cast('2020-01-01 02:30:00' as timestamp) as event_time
"""

_microbatch_hourly_sql = """
{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    event_time='event_time',
    batch_size='hour',
    begin=modules.datetime.datetime(2020, 1, 1, 0, 0, 0),
    partition_by=['hours(event_time)'],
    file_format='iceberg',
    concurrent_batches=false
) }}
select id, event_time from {{ ref('input_model') }}
"""


class TestMicrobatchHourlySession:
    """Hourly microbatch over two batches on `method: session` must not fail on the second hour."""

    @pytest.fixture(scope="class")
    def models(self, local_session_profile):
        return {
            "input_model.sql": _input_model_sql,
            "microbatch_hourly.sql": _microbatch_hourly_sql,
        }

    def _run_two_hours(self):
        results = run_dbt(
            [
                "run",
                "--event-time-start",
                "2020-01-01T00:00:00",
                "--event-time-end",
                "2020-01-01T02:00:00",
            ]
        )
        microbatch = [r for r in results if r.node.name == "microbatch_hourly"]
        assert len(microbatch) == 1
        result = microbatch[0]
        assert result.status == RunStatus.Success, result.message
        assert len(result.batch_results.successful) == 2
        assert result.batch_results.failed == []

    def test_two_hourly_batches_succeed(self, project):
        relation = relation_from_name(project.adapter, "microbatch_hourly")

        self._run_two_hours()
        rows = project.run_sql(f"select id from {relation} order by id", fetch="all")
        assert [row[0] for row in rows] == [1, 2, 3, 4]

        self._run_two_hours()
        rows = project.run_sql(f"select id from {relation} order by id", fetch="all")
        assert [row[0] for row in rows] == [1, 2, 3, 4]
