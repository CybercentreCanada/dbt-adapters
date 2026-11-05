import os
from pathlib import Path
import subprocess

import pytest


SPARK_ROOT = Path(__file__).resolve().parents[2]
RUNNER = SPARK_ROOT / "scripts" / "run_local_tests.sh"


@pytest.fixture
def runner(tmp_path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    log = tmp_path / "commands.log"
    for tool, body in {
        "docker": """
echo "docker $*" >> "$COMMAND_LOG"
case "$*" in
    info) exit "${DOCKER_STATUS:-0}" ;;
    *"ps --all --quiet")
        if [ "${EXISTING_CATALOG:-0}" = 1 ]; then echo existing-container; fi ;;
    *"up -d") exit "${UP_STATUS:-0}" ;;
    *down) exit "${DOWN_STATUS:-0}" ;;
esac
""",
        "curl": """
if [ "${EXISTING_ENDPOINT:-0}" = 1 ]; then exit 0; fi
if [ ! -f "$CURL_SEEN" ]; then touch "$CURL_SEEN"; exit 1; fi
exit "${READINESS_STATUS:-0}"
""",
        "hatch": """
echo "hatch $* cwd=$PWD ip=$SPARK_LOCAL_IP" >> "$COMMAND_LOG"
if [ "${FAIL_SUITE:-}" = "$2" ]; then exit 7; fi
""",
        "java": "exit 0",
        "sleep": "exit 0",
    }.items():
        executable = bin_dir / tool
        executable.write_text("#!/usr/bin/env bash\n" + body + "\n")
        executable.chmod(0o755)
    env = {
        **os.environ,
        "PATH": f"{bin_dir}:{os.environ['PATH']}",
        "COMMAND_LOG": str(log),
        "CURL_SEEN": str(tmp_path / "curl-seen"),
        "SPARK_LOCAL_IP": "127.0.0.1",
    }

    def run(**overrides):
        result = subprocess.run(
            ["bash", str(RUNNER), "-q"],
            cwd=tmp_path,
            env={**env, **overrides},
            text=True,
            capture_output=True,
            timeout=10,
        )
        return result, log.read_text()

    return run


def test_runner_runs_all_suites_from_module_and_cleans_up(runner):
    result, log = runner()
    assert result.returncode == 0, result.stderr
    for target in [
        "local-spark-iceberg:adapter-tests",
        "local-spark-iceberg:functional-tests",
        "local-spark-iceberg:unit-tests",
        "local-spark-tests:test",
    ]:
        assert f"hatch run {target} -q cwd={SPARK_ROOT} ip=127.0.0.1" in log
    assert log.count("up -d") == 1
    assert log.count("down") == 1


@pytest.mark.parametrize(
    "suite",
    [
        "local-spark-iceberg:adapter-tests",
        "local-spark-iceberg:functional-tests",
        "local-spark-iceberg:unit-tests",
        "local-spark-tests:test",
    ],
)
def test_runner_continues_after_test_failure_and_exits_nonzero(runner, suite):
    result, log = runner(FAIL_SUITE=suite)
    assert result.returncode == 1
    assert "FAIL:" in result.stderr
    assert log.count("hatch run") == 4
    assert log.endswith("docker compose -f docker/compose.local-spark-iceberg.yml down\n")


@pytest.mark.parametrize(
    "overrides",
    [{"EXISTING_CATALOG": "1"}, {"EXISTING_ENDPOINT": "1"}, {"DOCKER_STATUS": "1"}],
)
def test_runner_does_not_touch_existing_services_or_run_without_docker(runner, overrides):
    result, log = runner(**overrides)
    assert result.returncode != 0
    assert "up -d" not in log
    assert "down" not in log
    assert "hatch run" not in log


@pytest.mark.parametrize("overrides", [{"UP_STATUS": "1"}, {"READINESS_STATUS": "1"}])
def test_runner_cleans_up_failed_catalog_startup(runner, overrides):
    result, log = runner(**overrides)
    assert result.returncode != 0
    assert "down" in log
    assert "hatch run" not in log


def test_runner_reports_cleanup_failure(runner):
    result, log = runner(DOWN_STATUS="1")
    assert result.returncode == 1
    assert "Failed to stop" in result.stderr
    assert log.count("hatch run") == 4
