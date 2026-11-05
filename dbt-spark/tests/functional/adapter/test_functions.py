import os
import sys

import pytest

from dbt.tests.util import run_dbt
from dbt.adapters.spark.session import get_spark_session


class TestSessionFunctions:
    @pytest.fixture(scope="class")
    def dbt_profile_target(self, request):
        if request.config.getoption("--profile") != "spark_session":
            pytest.skip("Requires --profile spark_session")
        return {
            "type": "spark",
            "method": "session",
            "host": "localhost",
            "server_side_parameters": {
                "spark.master": "local[2]",
                "spark.driver.host": "127.0.0.1",
                "spark.driver.bindAddress": "127.0.0.1",
                "spark.ui.enabled": "false",
            },
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"require_location_root": False},
            "models": {
                "+database": "spark_catalog",
                "+file_format": "parquet",
                "+submission_method": "spark_session_based_cluster",
            },
            "functions": {"+spark_function_scope": "session"},
        }

    @pytest.fixture(scope="class")
    def functions(self):
        runtime = f"{sys.version_info.major}.{sys.version_info.minor}"
        return {
            "sql_double.sql": "value * 2",
            "py_multiply.py": """
if value is None:
    return None
return value * multiplier
""",
            "schema.yml": f"""
functions:
  - name: sql_double
    arguments:
      - name: value
        data_type: INT
    returns:
      data_type: INT
  - name: py_multiply
    config:
      runtime_version: "{runtime}"
      entry_point: implementation
    arguments:
      - name: value
        data_type: INT
      - name: multiplier
        data_type: INT
    returns:
      data_type: INT
""",
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "function_alias.sql": """
{% macro generate_alias_name(custom_alias_name=none, node=none) -%}
    {%- if node.resource_type == 'function' and node.name == 'sql_double' -%}
        physical_sql_double
    {%- else -%}
        {{ custom_alias_name or node.name }}
    {%- endif -%}
{%- endmacro %}
"""
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "consumer.sql": """
{{ config(materialized='table') }}
select {{ function('sql_double') }}(3) as sql_value,
       {{ function('py_multiply') }}(3, 4) as py_value
""",
            "python_consumer.py": """
def model(dbt, spark):
    dbt.config(materialized="table")
    upstream = dbt.ref("consumer")
    return upstream.selectExpr(
        "`physical_sql_double`(5) as sql_value", "`py_multiply`(5, 4) as py_value"
    )
""",
        }

    def test_native_build_compile_render_results_and_cleanup(self, project):
        parameters = project.adapter.config.credentials.server_side_parameters
        spark = get_spark_session(parameters)
        assert spark.version == "4.1.3"
        try:
            parsed = run_dbt(["parse"])
            assert parsed is not None
            assert not spark.catalog.functionExists("physical_sql_double")
            built = run_dbt(["build", "--threads", "4"])
            assert len(built.results) == 4
            assert all(result.status == "success" for result in built.results)
            function_results = [
                result for result in built.results if result.node.resource_type == "function"
            ]
            assert len(function_results) == 2
            assert all(result.adapter_response["_message"] == "OK" for result in function_results)
            for name, expected in [("consumer", (6, 12)), ("python_consumer", (10, 20))]:
                result = spark.sql(f"select * from {project.test_schema}.{name}").first()
                assert tuple(result) == expected
                if parameters.get("spark.sql.catalog.polaris.type") == "rest":
                    assert spark.catalog.currentCatalog() == "polaris"
                    metadata = spark.sql(
                        f"describe extended polaris.{project.test_schema}.{name}"
                    ).collect()
                    assert any(row[0] == "Provider" and row[1] == "iceberg" for row in metadata)
            assert not spark.catalog.functionExists("physical_sql_double")
            assert not spark.catalog.functionExists("py_multiply")
            compiled = run_dbt(["compile", "--select", "consumer"])
            assert "`physical_sql_double`" in compiled.results[0].node.compiled_code
            assert not spark.catalog.functionExists("physical_sql_double")
            failed = run_dbt(["run", "--select", "consumer"], expect_pass=False)
            assert failed.results[0].status == "error"
            assert "not built in this invocation" in failed.results[0].message
            rebuilt = run_dbt(["build", "--select", "+consumer", "--threads", "2"])
            assert len(rebuilt.results) == 3
            assert all(result.status == "success" for result in rebuilt.results)
        finally:
            for name in ("consumer", "python_consumer"):
                spark.sql(f"drop table if exists {project.test_schema}.{name}")


class TestPolarisSessionFunctions(TestSessionFunctions):
    @pytest.fixture(scope="class")
    def dbt_profile_target(self, request):
        if request.config.getoption("--profile") != "polaris":
            pytest.skip("Requires --profile polaris")
        uri = os.getenv("DBT_SPARK_POLARIS_URI")
        warehouse = os.getenv("DBT_SPARK_POLARIS_CATALOG")
        if not uri or not warehouse:
            pytest.skip("Set DBT_SPARK_POLARIS_URI and DBT_SPARK_POLARIS_CATALOG")
        params = {
            "spark.master": "local[2]",
            "spark.driver.host": "127.0.0.1",
            "spark.driver.bindAddress": "127.0.0.1",
            "spark.jars.packages": (
                "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.12.0,"
                "org.apache.iceberg:iceberg-aws-bundle:1.12.0"
            ),
            "spark.sql.catalog.polaris": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.polaris.type": "rest",
            "spark.sql.catalog.polaris.uri": uri,
            "spark.sql.catalog.polaris.warehouse": warehouse,
            "spark.sql.catalog.polaris.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
            "spark.sql.defaultCatalog": "polaris",
        }
        endpoint = os.getenv("DBT_SPARK_POLARIS_S3_ENDPOINT")
        if endpoint:
            params.update(
                {
                    "spark.sql.catalog.polaris.s3.endpoint": endpoint,
                    "spark.sql.catalog.polaris.s3.path-style-access": "true",
                    "spark.sql.catalog.polaris.client.region": "us-west-2",
                    "spark.sql.catalog.polaris.s3.access-key-id": os.environ[
                        "DBT_SPARK_POLARIS_S3_ACCESS_KEY"
                    ],
                    "spark.sql.catalog.polaris.s3.secret-access-key": os.environ[
                        "DBT_SPARK_POLARIS_S3_SECRET_KEY"
                    ],
                }
            )
        credential = os.getenv("DBT_SPARK_POLARIS_CREDENTIAL")
        if credential:
            params.update(
                {
                    "spark.sql.catalog.polaris.rest.auth.type": "oauth2",
                    "spark.sql.catalog.polaris.credential": credential,
                    "spark.sql.catalog.polaris.scope": os.getenv(
                        "DBT_SPARK_POLARIS_SCOPE", "PRINCIPAL_ROLE:ALL"
                    ),
                    "spark.sql.catalog.polaris.oauth2-server-uri": os.getenv(
                        "DBT_SPARK_POLARIS_TOKEN_URI", uri.rstrip("/") + "/v1/oauth/tokens"
                    ),
                }
            )
        else:
            params["spark.sql.catalog.polaris.rest.auth.type"] = "none"
        return {
            "type": "spark",
            "method": "session",
            "host": "localhost",
            "server_side_parameters": params,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"require_location_root": False, "spark_table_only": True},
            "models": {
                "+database": "polaris",
                "+file_format": "iceberg",
                "+submission_method": "spark_session_based_cluster",
            },
            "functions": {"+spark_function_scope": "session"},
        }
