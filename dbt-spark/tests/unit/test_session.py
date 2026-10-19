from unittest import mock

from dbt.adapters.spark.session import Cursor, get_spark_session


def test_session_parameters_are_applied_before_initialization():
    parameters = {
        "spark.sql.catalog.polaris": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.extensions": (
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        ),
    }
    with mock.patch("dbt.adapters.spark.session.SparkSession") as spark_session:
        builder = spark_session.builder
        builder.config.return_value = builder
        result = get_spark_session(parameters)

    assert result is builder.getOrCreate.return_value
    assert builder.mock_calls == [
        mock.call.config(key, value) for key, value in parameters.items()
    ] + [mock.call.getOrCreate()]


def test_cursor_uses_configured_session():
    parameters = {"spark.sql.defaultCatalog": "polaris"}
    cursor = Cursor(server_side_parameters=parameters)
    with mock.patch("dbt.adapters.spark.session.get_spark_session") as get_session:
        cursor.execute("select 1")

    get_session.assert_called_once_with(parameters)
    get_session.return_value.sql.assert_called_once_with("select 1")
