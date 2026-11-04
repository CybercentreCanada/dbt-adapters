import pytest
import yaml

from dbt.tests.util import write_file

from tests.functional.fixtures import (
    dbt_profile_target,
    skip_by_profile_type,
    start_databricks_cluster,
)


pytest_plugins = ["dbt.tests.fixtures.project"]


@pytest.fixture(scope="class")
def dbt_project_yml(dbt_project_yml, request, project_root):
    if request.config.getoption("--profile") == "local_spark_iceberg":
        models = dbt_project_yml.setdefault("models", {})
        models.setdefault("+database", "iceberg")
        models.setdefault("+file_format", "iceberg")
        dbt_project_yml.setdefault("flags", {})["require_location_root"] = False
        write_file(yaml.safe_dump(dbt_project_yml), project_root, "dbt_project.yml")
    return dbt_project_yml


@pytest.fixture(scope="class")
def project(project, request):
    local_iceberg = request.config.getoption("--profile") == "local_spark_iceberg"
    if local_iceberg:
        project.database = "iceberg"
    yield project
    if local_iceberg:
        adapter = project.adapter
        schema = adapter.Relation.create(database="iceberg", schema=project.test_schema)
        with adapter.connection_named("local_iceberg_cleanup"):
            for relation in adapter.list_relations_without_caching(schema):
                adapter.drop_relation(relation)


def pytest_addoption(parser):
    parser.addoption("--profile", action="store", default="apache_spark", type=str)


# Using @pytest.mark.skip_profile('apache_spark') uses the 'skip_by_profile_type'
# autouse fixture below
def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "skip_profile(profile): skip test for the given profile",
    )
