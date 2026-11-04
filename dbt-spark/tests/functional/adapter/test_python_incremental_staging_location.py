import shutil
from pathlib import Path
from unittest.mock import PropertyMock, patch
from urllib.parse import unquote, urlparse
from uuid import uuid4

import pytest

from dbt.adapters.spark import SparkAdapter
from dbt.adapters.spark.python_submissions import SparkSessionBasedClusterPythonJobHelper
from dbt.adapters.spark.session import get_spark_session
from dbt.tests.util import run_dbt, write_file


def _model_sql(incremental=False):
    rows = "select 1 as id, 'updated' as value union all select 2, 'new'"
    if not incremental:
        rows = "select 1 as id, 'original' as value"
    return (
        "def model(dbt, spark):\n"
        "    dbt.config(materialized='incremental')\n"
        f'    return spark.sql("{rows}")\n'
    )


def _location(spark, relation):
    metadata = spark.sql(f"describe extended {relation}").collect()
    value = next(row[1] for row in metadata if row[0] == "Location")
    return Path(unquote(urlparse(value).path)).resolve()


@pytest.fixture(scope="class")
def location_root(request):
    if request.config.getoption("--profile") != "local_spark_iceberg":
        pytest.skip("Requires the local Spark Iceberg integration profile")
    warehouse = Path(__file__).resolve().parents[3] / ".hatch/local-spark-iceberg/warehouse"
    return warehouse / "python-staging" / uuid4().hex


class _PythonIncrementalStaging:
    @pytest.fixture(scope="class")
    def models(self):
        return {"orders.py": _model_sql()}

    @pytest.fixture(scope="class")
    def project_config_update(self, location_root):
        return {
            "models": {
                "+materialized": "incremental",
                "+incremental_strategy": "merge",
                "+unique_key": "id",
                "+submission_method": self.submission_method,
                "+file_format": "iceberg",
                "+location_root": location_root.as_uri(),
            }
        }

    @pytest.fixture(scope="class")
    def submissions(self, project, location_root):
        observations = []
        credentials = project.adapter.config.credentials
        spark = get_spark_session(credentials.server_side_parameters)
        namespace = f"iceberg.{project.test_schema}"
        target = f"{namespace}.orders"
        stage = f"{namespace}.orders__dbt_tmp"
        helpers = project.adapter.python_submission_helpers

        class ObservedLocalSubmission(SparkSessionBasedClusterPythonJobHelper):
            def submit(self, compiled_code):
                super().submit(compiled_code)
                observation = {"target_location": _location(spark, target)}
                staging_name = (
                    "orders__dbt_tmp" if spark.catalog.tableExists("orders__dbt_tmp") else stage
                )
                if spark.catalog.tableExists(staging_name):
                    staging = spark.catalog.getTable(staging_name)
                    observation["temporary"] = staging.isTemporary
                    if not staging.isTemporary:
                        observation["stage_location"] = _location(spark, stage)
                observations.append(observation)

        helpers[self.submission_method] = ObservedLocalSubmission
        location_root.mkdir(parents=True)
        try:
            with patch.object(
                SparkAdapter,
                "python_submission_helpers",
                new_callable=PropertyMock,
                return_value=helpers,
            ):
                yield spark, observations
        finally:
            spark.catalog.dropTempView("orders__dbt_tmp")
            with project.adapter.connection_named("python_staging_cleanup"):
                project.adapter.execute(f"drop table if exists {stage}")
                project.adapter.execute(f"drop table if exists {target}")
            shutil.rmtree(location_root)

    def test_existing_target_staging_preserves_locations_and_merges(
        self, project, submissions, location_root
    ):
        spark, observations = submissions
        target = f"iceberg.{project.test_schema}.orders"
        stage = f"iceberg.{project.test_schema}.orders__dbt_tmp"
        assert len(run_dbt(["run"])) == 1
        assert [(row.id, row.value) for row in spark.table(target).collect()] == [(1, "original")]
        assert observations == [{"target_location": location_root / "orders"}]

        write_file(_model_sql(incremental=True), project.project_root, "models", "orders.py")
        assert len(run_dbt(["run"])) == 1
        assert len(observations) == 2
        second = observations[1]
        assert second["target_location"] == location_root / "orders"
        assert second["temporary"] is self.temporary
        if self.temporary:
            assert "stage_location" not in second
            assert not (location_root / "orders__dbt_tmp").exists()
        else:
            assert second["stage_location"] == location_root / "orders__dbt_tmp"
            assert second["stage_location"] != second["target_location"]

        assert [(row.id, row.value) for row in spark.table(target).orderBy("id").collect()] == [
            (1, "updated"),
            (2, "new"),
        ]
        assert _location(spark, target) == location_root / "orders"
        assert not spark.catalog.tableExists(stage)
        assert spark.catalog.tableExists("orders__dbt_tmp") is self.temporary
        if self.temporary:
            assert spark.catalog.getTable("orders__dbt_tmp").isTemporary


class TestLocalPhysicalPythonStaging(_PythonIncrementalStaging):
    submission_method = "local_physical_test"
    temporary = False


class TestLocalTemporaryViewPythonStaging(_PythonIncrementalStaging):
    submission_method = "spark_session_based_cluster"
    temporary = True
