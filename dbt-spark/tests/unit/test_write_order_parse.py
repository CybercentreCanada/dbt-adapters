import json

import pytest
from dbt.cli.main import dbtRunner


def write_project(
    path, materialized="table", language="sql", write_order=None, extra=None, query=None
):
    models = path / "models"
    models.mkdir(exist_ok=True)
    (path / "dbt_project.yml").write_text(
        "name: write_order_test\nversion: '1.0'\nconfig-version: 2\nprofile: local\n"
        "flags:\n  require_location_root: false\n"
    )
    (path / "profiles.yml").write_text(
        "local:\n  target: test\n  outputs:\n    test:\n"
        "      type: spark\n      method: session\n      host: localhost\n"
        "      schema: default\n      threads: 2\n"
    )
    config = {"materialized": materialized, "file_format": "iceberg", "database": "test_catalog"}
    config.update(extra or {})
    if language == "python":
        config["submission_method"] = "spark_session_based_cluster"
    if write_order is not None:
        config["write_order"] = write_order
    if language == "sql":
        code = (
            "{{ config("
            + ", ".join(f"{key}={value!r}" for key, value in config.items())
            + ") }}\n"
        )
        code += query or "select 1 as customer_id, 2 as total"
    else:
        code = (
            "def model(dbt, spark):\n"
            "    dbt.config("
            + ", ".join(f"{key}={value!r}" for key, value in config.items())
            + ")\n"
            f"    return spark.sql({(query or 'select 1 as customer_id, 2 as total')!r})\n"
        )
    (models / f"orders.{language if language == 'sql' else 'py'}").write_text(code)
    return config


@pytest.mark.parametrize("language", ["sql", "python"])
def test_model_parse_retains_write_order_configuration(tmp_path, language):
    write_project(tmp_path, language=language, write_order=["customer_id DESC"])
    result = dbtRunner().invoke(
        [
            "parse",
            "--no-partial-parse",
            "--project-dir",
            str(tmp_path),
            "--profiles-dir",
            str(tmp_path),
            "--log-path",
            str(tmp_path / "logs"),
        ]
    )
    assert result.success, str(result.exception)
    manifest = json.loads((tmp_path / "target" / "manifest.json").read_text())
    assert manifest["nodes"]["model.write_order_test.orders"]["config"]["write_order"] == [
        "customer_id DESC"
    ]


@pytest.mark.parametrize("materialized", ["ephemeral", "view", "streaming"])
def test_parse_rejects_explicit_order_on_unsupported_materializations(tmp_path, materialized):
    write_project(tmp_path, materialized=materialized, write_order=[])
    result = dbtRunner().invoke(
        [
            "parse",
            "--no-partial-parse",
            "--project-dir",
            str(tmp_path),
            "--profiles-dir",
            str(tmp_path),
            "--log-path",
            str(tmp_path / "logs"),
        ]
    )
    assert not result.success
    assert "write_order" in str(result.exception)
