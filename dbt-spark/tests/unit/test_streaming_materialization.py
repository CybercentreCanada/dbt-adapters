"""Unit tests for the Spark Python streaming materialization template."""

from pathlib import Path

import pytest
from jinja2 import Environment


STREAMING_TEMPLATE = (
    Path(__file__).parents[2] / "src/dbt/include/spark/macros/materializations/streaming.sql"
)


def _source() -> str:
    return STREAMING_TEMPLATE.read_text()


class _CompilerError(Exception):
    pass


def _raise_compiler_error(message):
    raise _CompilerError(message)


def _stream_options(config):
    environment = Environment(extensions=["jinja2.ext.do"])
    source = _source().replace(
        "{% materialization streaming, adapter='spark', supported_languages=['python'] %}",
        "{% macro streaming() %}",
    )
    source = source.replace("{% endmaterialization %}", "{% endmacro %}")
    template = environment.from_string(source)
    template.globals.update(
        {
            "config": type(
                "Config", (), {"get": lambda _, key, default=None: config.get(key, default)}
            )(),
            "exceptions": type(
                "Exceptions", (), {"raise_compiler_error": staticmethod(_raise_compiler_error)}
            )(),
            "return": lambda value: value,
            "spark__escape_single_quotes": lambda value: value.replace("'", "\\'"),
        }
    )
    return template.module.spark__stream_options_clause()


def test_streaming_materialization_template_parses():
    source = _source().replace(
        "{% materialization streaming, adapter='spark', supported_languages=['python'] %}",
        "{% macro streaming() %}",
    )
    source = source.replace("{% endmaterialization %}", "{% endmacro %}")

    Environment(extensions=["jinja2.ext.do"]).parse(source)


def test_streaming_materialization_is_local_session_python_only():
    source = _source()

    assert "supported_languages=['python']" in source
    assert "submission_method='spark_session_based_cluster'" in source


def test_streaming_materialization_reuses_active_query_before_model_execution():
    source = _source()

    active_query_guard = source.index("active_query = next(")
    model_execution = source.index("df = model(dbt, spark)")

    assert active_query_guard < model_execution
    assert "skipping startup" in source
    assert "Changed stream_options take effect after the stream is restarted" in source


def test_streaming_materialization_creates_v2_sink_and_syncs_metadata():
    source = _source()

    assert "spark.createDataFrame([], df.schema).writeTo(target_name)" in source
    assert ".using(\"{{ config.get('file_format', 'delta') }}\")" in source
    assert "python__partitionedBy_clause" in source
    assert "python__tblproperties_clause" in source
    assert "adapter.check_partition_sync" in source
    assert "sync_tblproperties" in source
    assert "persist_docs(target_relation, model)" in source


def test_streaming_materialization_only_waits_when_configured():
    source = _source()

    assert "adapter.behavior.await_termination" in source
    assert "if {{ await_termination }}:" in source
    assert "active_query.awaitTermination()" in source


def test_stream_options_render_on_data_stream_writer():
    rendered = _stream_options(
        {
            "file_format": "iceberg",
            "stream_options": {
                "fanout-enabled": "true",
                "check-nullability": "true",
                "check-ordering": "false",
                "snapshot-property.app-id": "my_supervisor_v1",
            },
        }
    )

    assert '.option("fanout-enabled", "true")' in rendered
    assert '.option("check-nullability", "true")' in rendered
    assert '.option("check-ordering", "false")' in rendered
    assert '.option("snapshot-property.app-id", "my_supervisor_v1")' in rendered
    assert _source().index("spark__stream_options_clause") > _source().index("df.writeStream")


@pytest.mark.parametrize(
    "config, message",
    [
        ({"file_format": "delta", "stream_options": {"fanout-enabled": "true"}}, "iceberg"),
        ({"file_format": "iceberg", "stream_options": ["fanout-enabled"]}, "dictionary"),
        ({"file_format": "iceberg", "stream_options": {"fanout-enabled": True}}, "true"),
        (
            {"file_format": "iceberg", "stream_options": {"snapshot-property.": "value"}},
            "Unsupported",
        ),
        ({"file_format": "iceberg", "stream_options": {"unknown": "value"}}, "Unsupported"),
    ],
)
def test_stream_options_reject_invalid_configuration(config, message):
    with pytest.raises(_CompilerError, match=message):
        _stream_options(config)
