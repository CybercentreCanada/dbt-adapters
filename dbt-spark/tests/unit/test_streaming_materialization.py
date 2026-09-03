"""Unit tests for the Spark Python streaming materialization template."""

from pathlib import Path

import pytest
from jinja2 import Environment

from dbt.adapters.spark.sql_rewrite import rewrite_streaming_sql

STREAMING_TEMPLATE = (
    Path(__file__).parents[2] / "src/dbt/include/spark/macros/materializations/streaming.sql"
)


def _source() -> str:
    return STREAMING_TEMPLATE.read_text()


def _jinja_source() -> str:
    source = _source().replace(
        "{% materialization streaming, adapter='spark', supported_languages=['sql', 'python'] %}",
        "{% macro streaming() %}",
    )
    return source.replace("{% endmaterialization %}", "{% endmacro %}")


class _CompilerError(Exception):
    pass


def _raise_compiler_error(message):
    raise _CompilerError(message)


def _write_stream_options(config):
    environment = Environment(extensions=["jinja2.ext.do"])
    template = environment.from_string(_jinja_source())
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
    return template.module.spark__write_stream_options_clause()


def _read_stream_options(config):
    environment = Environment(extensions=["jinja2.ext.do"])
    template = environment.from_string(_jinja_source())
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
    return template.module.spark__read_stream_options_clause("reader")


def _render_stream_table(
    macro_name: str,
    await_termination: bool = False,
    location_root: str | None = None,
    partition_by: list[str] | None = None,
    tblproperties: dict[str, str] | None = None,
    read_stream_options: dict[str, str] | None = None,
) -> str:
    render_config = {
        "location_root": location_root,
        "partition_by": partition_by,
        "read_stream_options": read_stream_options or {},
        "stream_options": None,
        "tblproperties": tblproperties,
        "write_stream_options": {},
    }
    environment = Environment(extensions=["jinja2.ext.do"])
    template = environment.from_string(_jinja_source())
    template.globals.update(
        {
            "config": type(
                "Config", (), {"get": lambda _, key, default=None: render_config.get(key, default)}
            )(),
            "adapter": type(
                "Adapter",
                (),
                {
                    "behavior": type(
                        "Behavior",
                        (),
                        {
                            "await_termination": type(
                                "BehaviorFlag", (), {"no_warn": await_termination}
                            )()
                        },
                    )()
                },
            )(),
            "env_var": lambda _, default: default,
            "location_clause": lambda: "",
            "model": type("Model", (), {"refs": [], "sources": []})(),
            "python__partitionedBy_clause": lambda: (
                '  writer = writer.partitionedBy(days("ingest_loaded_by"))'
                if partition_by == ["days(ingest_loaded_by)"]
                else ""
            ),
            "python__location_clause": lambda: (
                'target_location = "' + location_root + '/stream"\n'
                'writer = writer.option("location", target_location)'
                if location_root
                else ""
            ),
            "python__tblproperties_clause": lambda: (
                '  writer = writer.tableProperty("owner", "analytics")'
                if tblproperties == {"owner": "analytics"}
                else ""
            ),
            "return": lambda value: value,
            "spark__escape_single_quotes": lambda value: value.replace("'", "\\'"),
        }
    )
    macro = getattr(template.module, macro_name)
    compiled_code = (
        "def model(dbt, spark):\n    return spark.readStream.table('source')"
        if macro_name == "py_stream_table"
        else "select 1"
    )
    return macro(compiled_code, "analytics.stream", False)


def test_streaming_materialization_template_parses():
    Environment(extensions=["jinja2.ext.do"]).parse(_jinja_source())


def test_streaming_materialization_is_local_session_only():
    source = _source()

    assert "supported_languages=['sql', 'python']" in source
    assert "submission_method='spark_session_based_cluster'" in source
    assert "statement('main', language=execution_language)" in source
    assert "sql_stream_table(compiled_code, target_relation" in source


def test_streaming_materialization_reuses_active_query_before_model_execution():
    source = _source()

    active_query_guard = source.index("active_query = next(")
    model_execution = source.index("df = model(dbt, spark)")

    assert active_query_guard < model_execution
    assert "skipping startup" in source
    assert "Changed write_stream_options and read_stream_options" in source


def test_streaming_materialization_creates_v2_sink_and_syncs_metadata():
    source = _source()

    assert "spark.createDataFrame([], {{ dataframe }}.schema).writeTo(target_name)" in source
    assert ".using(\"{{ config.get('file_format', 'delta') }}\")" in source
    assert "python__partitionedBy_clause" in source
    assert "python__tblproperties_clause" in source
    assert "adapter.check_partition_sync" in source
    assert "sync_tblproperties" in source
    assert "persist_docs(target_relation, model)" in source


@pytest.mark.parametrize("macro_name", ["py_stream_table", "sql_stream_table"])
@pytest.mark.parametrize("await_termination", [False, True])
def test_streaming_materialization_preserves_runtime_dataframe_name(macro_name, await_termination):
    rendered = _render_stream_table(macro_name, await_termination)

    compile(rendered, f"{macro_name}.py", "exec")

    assert "isinstance(df, pyspark.sql.dataframe.DataFrame)" in rendered
    assert "spark.createDataFrame([], df.schema)" in rendered
    assert "df.writeStream" in rendered
    assert f"if {await_termination}:" in rendered
    assert "BehaviorFlag" not in rendered


@pytest.mark.parametrize("macro_name", ["py_stream_table", "sql_stream_table"])
def test_streaming_materialization_uses_python_location_option(macro_name):
    rendered = _render_stream_table(macro_name, location_root="/mnt/root")

    compile(rendered, f"{macro_name}.py", "exec")

    assert 'target_location = "/mnt/root/stream"' in rendered
    assert 'writer = writer.option("location", target_location)' in rendered
    assert 'writer = writer.option("path",' not in rendered
    assert '.split("\'")' not in rendered


@pytest.mark.parametrize("macro_name", ["py_stream_table", "sql_stream_table"])
def test_streaming_materialization_quotes_partition_transform_columns(macro_name):
    rendered = _render_stream_table(macro_name, partition_by=["days(ingest_loaded_by)"])

    compile(rendered, f"{macro_name}.py", "exec")

    assert 'writer = writer.partitionedBy(days("ingest_loaded_by"))' in rendered
    assert "days(ingest_loaded_by)" not in rendered


@pytest.mark.parametrize("macro_name", ["py_stream_table", "sql_stream_table"])
def test_streaming_materialization_indents_shared_writer_helpers(macro_name):
    rendered = _render_stream_table(
        macro_name,
        partition_by=["days(ingest_loaded_by)"],
        tblproperties={"owner": "analytics"},
    )

    compile(rendered, f"{macro_name}.py", "exec")

    assert '        writer = writer.partitionedBy(days("ingest_loaded_by"))' in rendered
    assert '        writer = writer.tableProperty("owner", "analytics")' in rendered


@pytest.mark.parametrize("macro_name", ["py_stream_table", "sql_stream_table"])
def test_streaming_materialization_applies_read_stream_options_before_table(macro_name):
    rendered = _render_stream_table(macro_name, read_stream_options={"startSnapshotId": "12345"})

    compile(rendered, f"{macro_name}.py", "exec")

    option = 'reader = reader.option("startSnapshotId", "12345")'
    table = "reader.table("
    assert option in rendered
    assert rendered.index(option) < rendered.index(table)


@pytest.mark.parametrize(
    ("macro_name", "reader_indent"),
    [("py_stream_table", "        "), ("sql_stream_table", "        ")],
)
def test_streaming_materialization_renders_read_stream_options_on_separate_lines(
    macro_name, reader_indent
):
    read_stream_options = {
        "split-size": "8388608",
        "streaming-skip-delete-snapshots": "true",
        "startingVersion": "latest",
    }
    rendered = _render_stream_table(macro_name, read_stream_options=read_stream_options)

    compile(rendered, f"{macro_name}.py", "exec")

    for option, value in read_stream_options.items():
        assert f'\n{reader_indent}reader = reader.option("{option}", "{value}")\n' in rendered


def test_streaming_materialization_only_waits_when_configured():
    source = _source()

    assert "adapter.behavior.await_termination.no_warn" in source
    assert "if {{ await_termination }}:" in source
    assert "active_query.awaitTermination()" in source


def test_write_stream_options_render_on_data_stream_writer():
    rendered = _write_stream_options(
        {
            "file_format": "iceberg",
            "write_stream_options": {
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
    assert _source().index("spark__write_stream_options_clause") > _source().index(
        "{{ dataframe }}.writeStream"
    )


@pytest.mark.parametrize(
    "config, message",
    [
        ({"file_format": "delta", "write_stream_options": {"fanout-enabled": "true"}}, "iceberg"),
        ({"file_format": "iceberg", "write_stream_options": ["fanout-enabled"]}, "dictionary"),
        ({"file_format": "iceberg", "write_stream_options": {"fanout-enabled": True}}, "true"),
        (
            {"file_format": "iceberg", "write_stream_options": {"snapshot-property.": "value"}},
            "Unsupported",
        ),
        ({"file_format": "iceberg", "write_stream_options": {"unknown": "value"}}, "Unsupported"),
        ({"file_format": "iceberg", "write_stream_options": {1: "value"}}, "keys must be strings"),
    ],
)
def test_write_stream_options_reject_invalid_configuration(config, message):
    with pytest.raises(_CompilerError, match=message):
        _write_stream_options(config)


def test_stream_options_requires_renamed_configuration():
    with pytest.raises(_CompilerError, match="write_stream_options"):
        _write_stream_options({"stream_options": {"fanout-enabled": "true"}})


def test_read_stream_options_render_on_data_stream_reader():
    rendered = _read_stream_options(
        {
            "read_stream_options": {
                "stream-from-timestamp": "1628000000000",
                "startSnapshotId": "12345",
                "startingVersion": "latest",
                "streamingSkipDeleteSnapshots": "true",
                "streaming-max-rows-per-micro-batch": "500",
            }
        }
    )

    assert 'reader = reader.option("stream-from-timestamp", "1628000000000")' in rendered
    assert 'reader = reader.option("startSnapshotId", "12345")' in rendered
    assert 'reader = reader.option("startingVersion", "latest")' in rendered
    assert 'reader = reader.option("streamingSkipDeleteSnapshots", "true")' in rendered
    assert 'reader = reader.option("streaming-max-rows-per-micro-batch", "500")' in rendered


@pytest.mark.parametrize(
    "config, message",
    [
        ({"read_stream_options": ["startSnapshotId"]}, "dictionary"),
        ({"read_stream_options": {"unknown": "1"}}, "Unsupported"),
        ({"read_stream_options": {1: "1"}}, "keys must be strings"),
        ({"read_stream_options": {"start-snapshot-id": 1}}, "string value"),
        ({"read_stream_options": {"start-snapshot-id": "latest"}}, "numeric"),
        ({"read_stream_options": {"startingVersion": "invalid"}}, "latest"),
        ({"read_stream_options": {"streaming-skip-delete-snapshots": "yes"}}, "true"),
    ],
)
def test_read_stream_options_reject_invalid_configuration(config, message):
    with pytest.raises(_CompilerError, match=message):
        _read_stream_options(config)


def test_rewrite_streaming_sql_replaces_only_table_references():
    sql = """
        -- analytics.events must stay in this comment
        select event_id, 'analytics.events' as source_name
        from analytics.events as events
        join raw.clicks as clicks on events.event_id = clicks.event_id
    """

    rewritten = rewrite_streaming_sql(
        sql,
        {
            "analytics.events": "__dbt_stream_input_0",
            "raw.clicks": "__dbt_stream_input_1",
        },
    )

    assert "FROM __dbt_stream_input_0 AS events" in rewritten
    assert "JOIN __dbt_stream_input_1 AS clicks" in rewritten
    assert "'analytics.events' AS source_name" in rewritten
    assert "analytics.events must stay in this comment" in rewritten


def test_rewrite_streaming_sql_rejects_unmatched_input():
    with pytest.raises(RuntimeError, match="did not reference"):
        rewrite_streaming_sql("select * from analytics.events", {"raw.clicks": "input"})


def test_rewrite_streaming_sql_preserves_ctes_and_quoted_relation_aliases():
    rewritten = rewrite_streaming_sql(
        """
        with events as (
            select * from `analytics`.`raw events` as source_events
        )
        select * from events
        """,
        {"`analytics`.`raw events`": "__dbt_stream_input_0"},
    )

    assert "WITH events AS (SELECT * FROM __dbt_stream_input_0 AS source_events)" in rewritten
    assert "SELECT * FROM events" in rewritten
