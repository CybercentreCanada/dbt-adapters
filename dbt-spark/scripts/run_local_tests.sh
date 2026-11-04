#!/usr/bin/env bash

set -euo pipefail

if [[ "${1:-}" == "--help" ]]; then
    printf 'Usage: %s [pytest arguments...]\n' "$0"
    printf 'Run local Spark adapter, functional, unit, and live writer tests.\n'
    printf 'Starts and stops a dedicated Compose catalog; refuses existing catalogs.\n'
    exit 0
fi

cd "$(dirname "${BASH_SOURCE[0]}")/.."
export PWD
export SPARK_LOCAL_IP="${SPARK_LOCAL_IP:-127.0.0.1}"
export LOCAL_UID="$(id -u)" LOCAL_GID="$(id -g)"

for tool in docker curl hatch java; do
    if ! command -v "$tool" >/dev/null 2>&1; then
        printf 'Required tool not found: %s\n' "$tool" >&2
        exit 1
    fi
done

compose=(docker compose -f docker/compose.local-spark-iceberg.yml)
"${compose[@]}" version >/dev/null
docker info >/dev/null

existing=$("${compose[@]}" ps --all --quiet)
if [[ -n "$existing" ]]; then
    printf 'Existing local Iceberg Compose containers found; refusing to replace or stop them.\n' >&2
    exit 1
fi
catalog_url="http://127.0.0.1:8181/v1/config"
if curl --silent --fail --max-time 2 "$catalog_url" >/dev/null; then
    printf 'An Iceberg catalog is already listening on port 8181; refusing to take it over.\n' >&2
    exit 1
fi

cleanup() {
    status=$?
    trap - EXIT
    if ! "${compose[@]}" down; then
        printf 'Failed to stop the local Iceberg Compose catalog.\n' >&2
        if [[ "$status" -eq 0 ]]; then
            status=1
        fi
    fi
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

mkdir -p .hatch/local-spark-iceberg/warehouse
"${compose[@]}" up -d

ready=false
for ((attempt = 1; attempt <= 60; attempt++)); do
    if curl --silent --fail --max-time 2 "$catalog_url" >/dev/null; then
        ready=true
        break
    fi
    sleep 2
done
if [[ "$ready" != true ]]; then
    printf 'Local Iceberg catalog did not become ready.\n' >&2
    "${compose[@]}" logs catalog >&2
    exit 1
fi

failed=0
run_suite() {
    local name=$1
    shift
    printf '\n=== %s ===\n' "$name"
    if "$@"; then
        printf 'PASS: %s\n' "$name"
    else
        local status=$?
        printf 'FAIL: %s (exit %s)\n' "$name" "$status" >&2
        failed=1
    fi
}

run_suite "Adapter tests" hatch run local-spark-iceberg:adapter-tests "$@"
run_suite "Functional tests" hatch run local-spark-iceberg:functional-tests "$@"
run_suite "Session-compatible unit tests" hatch run local-spark-iceberg:unit-tests "$@"
run_suite "Local Spark writer tests" hatch run local-spark-tests:test "$@"
exit "$failed"
