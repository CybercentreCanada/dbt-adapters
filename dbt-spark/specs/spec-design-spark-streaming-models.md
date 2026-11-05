---
title: dbt-spark Structured Streaming Models
version: "1.0"
date_created: 2026-11-04
last_updated: 2026-11-04
tags: [design, dbt-spark, spark, streaming, iceberg]
---

# Introduction

This specification defines the current contract for streaming models in
`dbt-spark`, including model execution, input resolution, sink creation,
checkpointing, configuration, and known limitations. It separates behavior
already implemented from possible future capabilities.

## 1. Purpose & Scope

This specification is for `dbt-spark` maintainers, implementation agents, and
test authors. It describes the `streaming` materialization for SQL and Python
models that use Apache Spark Structured Streaming.

It covers the adapter behavior for starting or reusing a streaming query and
writing its output to a persistent table. It does not specify the semantics of
Spark's streaming engine, define guarantees for individual source or sink
connectors, or describe dbt-core's separate `microbatch` incremental strategy.
Microbatch processes bounded batches during a dbt invocation; the `streaming`
materialization starts a long-running Spark query.

## 2. Definitions

| Term | Definition |
| --- | --- |
| dbt-spark | The dbt adapter for Apache Spark and Databricks. |
| Structured Streaming | Spark's stream-processing engine for incrementally processing data with streaming DataFrames and Datasets. |
| Streaming model | A SQL or Python dbt model configured with `materialized='streaming'`. |
| Streaming query | The active Spark `StreamingQuery` started by a streaming model. |
| Target relation | The persistent table written by a streaming model. |
| Checkpoint | Spark-managed persistent state used to recover progress and state for a streaming query. |
| `ref()` / `source()` | dbt model and source references resolved to relations. |
| `spark_session_based_cluster` | The Spark submission method that executes model code in the Spark session managed by the adapter. |
| Iceberg | Apache Iceberg, a table format supported by dbt-spark and used by default for streaming sinks. |
| Processing-time trigger | A trigger that asks Spark to process available data at a configured time interval. |
| Streaming input option | A reader option applied to a dbt-managed streaming table read. |
| Streaming writer option | An option applied to the streaming writer when a new query is started. |
| Query reuse | The behavior of finding an active query with the target relation name and skipping model evaluation and query startup. |

## 3. Requirements, Constraints & Guidelines

### Current behavior requirements

- **REQ-001**: The `streaming` materialization shall support SQL and Python
  model languages.
- **REQ-002**: The materialization shall require
  `submission_method='spark_session_based_cluster'` and fail compilation when a
  different or missing submission method is configured.
- **REQ-003**: The evaluated model shall produce a PySpark DataFrame whose
  `isStreaming` property is true. The materialization shall fail explicitly for
  unsupported return types or non-streaming DataFrames.
- **REQ-004**: A new target shall be initialized as an empty table using the
  output DataFrame schema before the streaming query writes to it. The table
  shall use `file_format`, defaulting to `iceberg`, and shall apply configured
  partitioning, table options, location, and table properties at creation.
- **REQ-005**: The streaming query name shall be the target relation name, and
  its checkpoint location shall be derived as
  `<checkpoint_basedir>/<target_relation_name>`.
- **REQ-006**: `checkpoint_basedir` shall resolve from model configuration, then
  `DBT_STREAMING_CHECKPOINT_BASEDIR`, then the default
  `tmp/dbt-streaming-checkpoints`. The configured or resolved path must be
  stable and writable by Spark for query recovery.
- **REQ-007**: A new query shall use a processing-time trigger. `trigger` shall
  resolve from model configuration, then `DBT_STREAMING_TRIGGER`, then the
  default `60 seconds`.
- **REQ-008**: By default, dbt shall start the query and return without waiting.
  A model-level `await_termination` value shall override the
  `await_termination` behavior flag. When enabled, the materialization shall
  call `StreamingQuery.awaitTermination()`.
- **REQ-009**: If a query with the target relation name is already active in the
  Spark session, dbt shall reuse it, skip model evaluation and query startup,
  and report that changed read or write options require a query restart to take
  effect.
- **REQ-010**: SQL model `ref()` and `source()` relations shall be loaded using
  `spark.readStream.table()` and rewritten to internal temporary views before
  the compiled SQL is executed. Every declared SQL input must be referenced by
  the compiled query; otherwise execution shall fail with an explicit error.
- **REQ-011**: Python models may return streaming DataFrames directly. For
  table-valued `dbt.ref()` and `dbt.source()` reads managed by the streaming
  materialization, the adapter shall use streaming table reads and apply
  configured `read_stream_options`.
- **REQ-012**: `read_stream_options` shall be a mapping of supported option
  names to string values. Invalid container types, keys, value types, or
  option-specific formats shall fail compilation.
- **REQ-013**: `write_stream_options` shall be a mapping of supported Iceberg
  writer option names to string values. Boolean-like options shall accept only
  the strings `"true"` or `"false"`. Non-empty writer options shall be rejected
  when the sink format is not Iceberg.
- **REQ-014**: The legacy `stream_options` configuration shall fail with a
  compiler error directing users to `write_stream_options`.
- **REQ-015**: `read_stream_options` and `write_stream_options` shall be
  rejected on materializations other than `streaming`.
- **REQ-016**: For an existing target, the materialization shall check
  partition compatibility and synchronize configured table properties before
  starting or reusing the query. It shall persist configured grants,
  documentation, and constraints through the existing adapter mechanisms.

### Constraints

- **CON-001**: dbt-spark does not stop or restart active streaming queries.
  Changes to model code, `read_stream_options`, or `write_stream_options` do not
  take effect until the query is stopped outside this materialization and a new
  query is started.
- **CON-002**: Only the `spark_session_based_cluster` submission method is
  supported for streaming models.
- **CON-003**: SQL `ref()` and `source()` inputs are all treated as streaming
  inputs. SQL streaming models cannot use this materialization to join a
  dbt-managed batch relation as a batch dimension.
- **CON-004**: `read_stream_options` only affects adapter-managed streaming
  reads. It does not modify user-authored `spark.readStream` calls in Python
  model code.
- **CON-005**: The adapter's `read_stream_options` are an allowlisted set of
  Iceberg streaming-reader options. The adapter's `write_stream_options` are
  also restricted to Iceberg-specific options.
- **CON-006**: Checkpoint stability, checkpoint retention, and checkpoint
  cleanup are operational responsibilities; the materialization does not
  manage checkpoint deletion or migration.
- **CON-007**: A query may continue running after the dbt invocation returns
  when `await_termination` is false. dbt does not provide lifecycle
  supervision for that query.

### Known gaps and candidate capabilities

The following are limitations or candidate features, not requirements that the
current implementation already satisfies.

| ID | Priority | Gap or candidate capability | Current impact |
| --- | --- | --- | --- |
| **GAP-001** | High | Declarative query lifecycle management: stop, restart, update, and remove a query by target. | Configuration or code changes do not reconcile an already-active query; users must manage query restart externally. |
| **GAP-002** | High | A supported checkpoint lifecycle contract covering safe reset, migration, retention, and cleanup. | Users must coordinate checkpoint storage and query lifecycle themselves; deleting or changing a checkpoint may affect recovery semantics. |
| **GAP-003** | Medium | Remote or job-cluster submission support. | Persistent query management only works in the adapter's local Spark session. |
| **GAP-004** | Medium | Mixed streaming and batch inputs for SQL models, including explicit batch-dimension joins. | Every declared `ref()` and `source()` is converted into a streaming table read. |
| **GAP-005** | Medium | Trigger modes beyond processing-time intervals, such as available-now or once execution, if supported by the Spark version and intended product contract. | The `trigger` setting is rendered as `processingTime`; users cannot select other trigger APIs through this interface. |
| **GAP-006** | Medium | Adapter-managed stream status, failure reporting, and operational observability after dbt returns. | The adapter reports startup or reuse but does not supervise or expose ongoing query health through a dbt contract. |
| **GAP-007** | Medium | A defined schema-evolution and compatibility policy for an existing streaming sink. | Existing-target handling synchronizes partitions and table properties, but this materialization does not define a streaming schema-evolution contract. |
| **GAP-008** | Low | Common reader-option configuration for direct Python `spark.readStream` calls. | Users must configure those reads themselves; `read_stream_options` only applies to adapter-managed table reads. |
| **GAP-009** | Medium | Explicit full-refresh semantics for streaming targets and active queries. | The materialization does not stop an active query or define whether full refresh recreates the target, reuses it, or requires checkpoint replacement. |

### Guidelines

- **GUD-001**: Keep documented current behavior separate from candidate gaps;
  do not imply that a listed gap is implemented or committed for a release.
- **GUD-002**: Any query lifecycle feature must define behavior for query
  identity, checkpoint compatibility, failures, and concurrent dbt invocations
  before implementation.
- **GUD-003**: Add coverage for both SQL and Python paths when changing shared
  streaming behavior.
- **GUD-004**: Keep the streaming materialization distinct from dbt-core
  microbatch incremental execution in names, configuration, documentation, and
  tests.

## 4. Interfaces & Data Contracts

### Model configuration

| Configuration | Type | Default / resolution | Contract |
| --- | --- | --- | --- |
| `materialized` | string | Required | Must be `streaming`. |
| `submission_method` | string | Required | Must be `spark_session_based_cluster`. |
| `file_format` | string | `iceberg` | Format used to create the sink table if it does not exist. |
| `checkpoint_basedir` | string | Model config, `DBT_STREAMING_CHECKPOINT_BASEDIR`, `tmp/dbt-streaming-checkpoints` | Stable, Spark-writable checkpoint parent directory. |
| `trigger` | string | Model config, `DBT_STREAMING_TRIGGER`, `60 seconds` | Processing-time interval passed to the streaming writer. |
| `await_termination` | boolean | Model config, then behavior flag (default false) | If true, wait for query termination. |
| `partition_by` | list | None | Sink table partition columns or supported partition transforms. |
| `location_root` | string | None | Parent path used to derive the sink table location, subject to the `require_location_root` behavior flag. |
| `options` | mapping | None | Data-source options used when creating the new sink table. |
| `tblproperties` | mapping | None | Sink table properties applied at table creation and synchronized on existing targets. |
| `read_stream_options` | mapping of string to string | None | Allowlisted Iceberg reader options on adapter-managed input reads. |
| `write_stream_options` | mapping of string to string | None | Allowlisted Iceberg writer options when starting a new query. |
| `stream_options` | legacy mapping | Not supported | Fails compilation; use `write_stream_options`. |

### Supported Iceberg read options

Boolean options accept only `"true"` or `"false"`. Other listed values are
numeric strings, except `startingVersion`, which also accepts `"latest"`.

| Canonical spelling | Accepted alias |
| --- | --- |
| `stream-from-timestamp` | `streamFromTimestamp` |
| `start-snapshot-id` | `startSnapshotId` |
| `end-snapshot-id` | `endSnapshotId` |
| `startingVersion` | None |
| `split-size` | `splitSize` |
| `streaming-skip-delete-snapshots` | `streamingSkipDeleteSnapshots` |
| `streaming-skip-overwrite-snapshots` | `streamingSkipOverwriteSnapshots` |
| `streaming-max-files-per-micro-batch` | `streamingMaxFilesPerMicroBatch` |
| `streaming-max-rows-per-micro-batch` | `streamingMaxRowsPerMicroBatch` |

### Supported Iceberg write options

| Option | Value contract |
| --- | --- |
| `fanout-enabled` | `"true"` or `"false"` |
| `check-nullability` | `"true"` or `"false"` |
| `check-ordering` | `"true"` or `"false"` |
| `snapshot-property.<key>` | Any string value; `<key>` must be non-empty. |

### Execution contract

1. Resolve the target relation and inspect the Spark session for an active
   query named after that relation.
2. If the query exists, skip model evaluation and query startup. Apply existing
   target metadata synchronization and return or wait according to
   `await_termination`.
3. If no query exists, evaluate the Python model or prepare SQL streaming
   inputs and evaluate the rewritten SQL.
4. Require the evaluated result to be a streaming PySpark DataFrame.
5. If the target does not exist, create an empty table using the result schema
   and configured sink options.
6. Start `writeStream` with the target query name, a target-specific checkpoint,
   the configured Iceberg writer options, the processing-time trigger, and
   `toTable(target_relation)`.
7. Return the target relation from materialization. Wait for query termination
   only when configured.

The SQL input rewrite must replace declared table relations with temporary
views without replacing text in comments or string literals. The adapter uses
SQLGlot to parse and rewrite the SQL; parsing failures and declared inputs not
referenced by the model are errors.

## 5. Acceptance Criteria

- **AC-001 (REQ-001, REQ-002)**: Given a SQL or Python streaming model using the
  required submission method, when dbt compiles it, then the adapter accepts
  the model; when another submission method is configured, compilation fails
  with an explicit message.
- **AC-002 (REQ-003)**: Given a model returning a non-DataFrame or a batch
  DataFrame, when the streaming materialization evaluates it, then execution
  fails with an explicit type or streaming-state error.
- **AC-003 (REQ-004, REQ-005, REQ-006, REQ-007)**: Given a valid model and a
  missing target, when a new query starts, then the sink is created from the
  streaming DataFrame schema and the query uses the configured format,
  checkpoint path, target name, and processing-time trigger.
- **AC-004 (REQ-008)**: Given `await_termination` is false, when the query
  starts, then dbt does not call `awaitTermination()`; given it is true at model
  or behavior-flag scope, dbt waits on the active query.
- **AC-005 (REQ-009)**: Given an active query with the target relation name,
  when the materialization runs again, then it does not evaluate model code or
  start a second query and reports that updated stream options require a
  restart.
- **AC-006 (REQ-010)**: Given a SQL model with declared `ref()` and `source()`
  inputs, when compiled SQL references those relations, then each relation is
  read as a stream and rewritten to its internal view while aliases, CTEs,
  comments, and string literals remain semantically intact.
- **AC-007 (REQ-010)**: Given a declared SQL input that is not referenced or SQL
  that cannot be parsed for rewriting, when the model executes, then the
  adapter raises an explicit error.
- **AC-008 (REQ-011)**: Given a Python model using an adapter-managed
  table-valued `dbt.ref()` or `dbt.source()` input, when it reads that input,
  then it uses `spark.readStream` and applies configured read options.
- **AC-009 (REQ-012, REQ-013)**: Given malformed, unsupported, or
  format-incompatible read or write options, when the model compiles, then dbt
  reports a compiler error describing the invalid option.
- **AC-010 (REQ-014, REQ-015)**: Given legacy `stream_options` or streaming-only
  options on another materialization, when that model compiles, then dbt fails
  with a message naming the supported configuration.
- **AC-011 (REQ-016)**: Given an existing target, when dbt executes or reuses
  its query, then it performs the existing partition and table-property
  synchronization and persists configured grants, documentation, and
  constraints.
- **AC-012 (CON-001, CON-006, CON-007)**: Given a running query and changed
  model code or options, when dbt runs again, then the existing query is reused;
  the adapter does not stop it, restart it, migrate/delete its checkpoint, or
  claim to supervise it.
- **AC-013 (CON-003)**: Given a SQL streaming model with a dbt-managed batch
  relation among its declared inputs, when it is evaluated, then the adapter
  treats that relation as a streaming input; mixed batch/stream behavior is not
  promised by this contract.
- **AC-014 (GAP-001 through GAP-009)**: Given a proposed implementation for a
  listed gap, when it is reviewed against this specification, then it is treated
  as a separate feature requiring an explicit contract and acceptance criteria,
  not as existing behavior.

## 6. Test Automation Strategy

- **Test levels**:
  - Unit tests shall cover Jinja rendering, configuration validation, query
    reuse guards, supported option values, SQL relation rewriting, and error
    cases.
  - Opt-in local Spark tests shall exercise query startup, target table
    creation, query reuse, and partitioned Iceberg sink behavior.
  - Integration tests against external Spark deployments are not required by
    this specification because they require deployment-specific credentials
    and infrastructure.
- **Frameworks**: Use the existing pytest, Jinja2, SQLGlot, and PySpark test
  setup in `dbt-spark`; do not add a separate test framework.
- **Test data management**: Use unique temporary target names, isolated
  checkpoint directories, and temporary local Iceberg warehouses. Stop active
  test queries and stop the Spark session during fixture cleanup.
- **CI/CD integration**: Run the focused unit tests in the normal dbt-spark
  unit-test workflow. Keep local Spark tests opt-in because they require Java
  and may download Spark/Iceberg runtime dependencies.
- **Coverage requirements**: Cover both SQL and Python model paths, new and
  existing targets, active-query reuse, wait and non-wait behavior, valid and
  invalid configuration, and SQL rewrite edge cases. No numeric coverage
  threshold is defined.
- **Performance testing**: No adapter-specific throughput target is defined.
  Any query lifecycle, checkpoint, or input-rewrite change that adds work to
  startup should include a measured regression check appropriate to that
  change.
- **Focused commands**:

  ```bash
  cd dbt-spark
  hatch run unit-tests -- tests/unit/test_streaming_materialization.py
  hatch run local-spark-tests:test
  ```

## 7. Rationale & Context

The `streaming` materialization uses a persistent Spark query rather than
producing a bounded dbt-run result. It therefore needs a stable query identity,
a persistent sink, and a checkpoint location. Reusing an active query avoids
starting duplicate writers in the same Spark session, but it also means that a
dbt invocation is not a declarative query-reconciliation or lifecycle
controller.

SQL inputs are rewritten because `ref()` and `source()` are dbt relations while
the streaming model must read them through Spark's streaming reader. Requiring
every declared SQL input to appear in the model catches mismatches between dbt's
dependency graph and the actual streaming query.

Iceberg-specific reader and writer options are allowlisted to catch invalid
configuration early and avoid presenting connector-specific options as
portable Spark settings. Keeping microbatch separate prevents users and
implementers from conflating bounded incremental batches with long-running
Structured Streaming queries.

The gap list makes missing lifecycle, deployment, input-mixing, trigger,
observability, schema-evolution, and direct-reader configuration contracts
visible without silently expanding current support.

## 8. Dependencies & External Integrations

### External Systems

- **EXT-001**: Apache Spark Structured Streaming - Executes streaming
  DataFrames, maintains active queries, applies triggers, and writes to the
  configured table sink.
- **EXT-002**: Spark SQL catalog - Resolves the target relation and provides
  the table APIs used to create and write the sink.

### Third-Party Services

- **SVC-001**: No external service is required by the adapter contract.
  Deployments may use cloud object storage or a managed Spark service; those
  deployment-specific availability and service-level requirements are outside
  this specification.

### Infrastructure Dependencies

- **INF-001**: The Spark driver and session must remain available for the
  lifetime of a streaming query.
- **INF-002**: The checkpoint store must be reachable and writable by Spark,
  persist across driver restarts as required by the deployment, and remain
  stable for the query's lifetime.
- **INF-003**: The sink table's backing storage and catalog must support the
  configured Spark table writer and concurrent access requirements of the
  deployment.

### Data Dependencies

- **DAT-001**: Each adapter-managed streaming input must resolve to a table
  that the Spark catalog and the source provider can read as a streaming table.
- **DAT-002**: The model output schema must be accepted by the selected sink
  format and existing target. This specification does not define automatic
  schema evolution.

### Technology Platform Dependencies

- **PLT-001**: A Spark runtime with Structured Streaming, streaming DataFrame
  reads, processing-time triggers, and `writeStream.toTable()` support is
  required.
- **PLT-002**: A compatible Apache Iceberg Spark integration is required when
  using the default Iceberg sink or the Iceberg-specific streaming options.
- **PLT-003**: The `spark_session_based_cluster` adapter submission path is
  required.

### Compliance Dependencies

- **COM-001**: No specific regulatory or compliance framework is imposed by
  this adapter contract. Deployments remain responsible for securing source
  data, sink data, and checkpoint storage under their applicable policies.

## 9. Examples & Edge Cases

### Python streaming model

```python
def model(dbt, spark):
    dbt.config(
        materialized="streaming",
        submission_method="spark_session_based_cluster",
        file_format="iceberg",
        checkpoint_basedir="s3://example-bucket/dbt-checkpoints",
        trigger="30 seconds",
        write_stream_options={
            "fanout-enabled": "true",
            "snapshot-property.app-id": "stream-supervisor",
        },
    )

    return spark.readStream.table("raw.events")
```

In this example, the direct Python `spark.readStream` call is authored by the
model. Adapter `read_stream_options` are not automatically applied to it. The
user may set reader options in that Python call.

### SQL streaming model

```sql
{{ config(
    materialized="streaming",
    submission_method="spark_session_based_cluster",
    checkpoint_basedir="s3://example-bucket/dbt-checkpoints"
) }}

select event_id, event_time
from {{ source("raw", "events") }}
```

The declared source relation is read through Spark's streaming table reader,
rewritten to an internal temporary view, and queried with Spark SQL.

### Reused query with changed options

If a query for the target is active, a later dbt run skips model evaluation and
reuses that query. Changing `trigger`, `read_stream_options`, or
`write_stream_options` does not mutate the active query. The query must be
stopped externally before a new dbt run can start a query with the changed
configuration.

### Invalid configurations

```yaml
# Invalid: use the supported name.
stream_options:
  fanout-enabled: "true"

# Invalid: boolean values must be strings and an Iceberg option is not portable
# to this sink format.
file_format: parquet
write_stream_options:
  fanout-enabled: true
```

Both cases must fail compilation with actionable errors rather than silently
ignoring the configuration.

## 10. Validation Criteria

- The documented defaults and precedence for `checkpoint_basedir`, `trigger`,
  and `await_termination` match the materialization implementation.
- SQL and Python materializations both validate streaming DataFrame output and
  start a query with a target-derived name and checkpoint location.
- Unit tests exercise option validation, SQL rewrite behavior, query reuse, and
  `await_termination` behavior.
- The opt-in local Spark suite confirms a streaming query can write to a local
  Iceberg table and that a second execution reuses the active query.
- The known limitations and gap table do not claim unsupported behavior is
  implemented.
- Changes to the materialization update this specification and the streaming
  section of the dbt-spark README when their behavior or public configuration
  changes.

## 11. Related Specifications / Further Reading

- [dbt-spark streaming materialization documentation](../README.md#streaming-materialization-sql-and-python)
- [Streaming materialization macros](../src/dbt/include/spark/macros/materializations/streaming.sql)
- [SQL streaming relation rewrite](../src/dbt/adapters/spark/sql_rewrite.py)
- [Streaming materialization unit tests](../tests/unit/test_streaming_materialization.py)
- [Opt-in local Spark streaming tests](../tests/unit/test_streaming_materialization_live.py)
- [Apache Spark Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/streaming/index.html)
- [Apache Iceberg Spark Structured Streaming](https://iceberg.apache.org/docs/latest/spark-structured-streaming/)
