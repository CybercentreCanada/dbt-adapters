"""Unit tests for the Spark Python streaming materialization template."""

from pathlib import Path

from jinja2 import Environment


STREAMING_TEMPLATE = (
    Path(__file__).parents[2] / "src/dbt/include/spark/macros/materializations/streaming.sql"
)


def _source() -> str:
    return STREAMING_TEMPLATE.read_text()


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
