---
title: dbt-spark Session-Based Python Model Execution
version: "1.0"
date_created: 2026-11-04
last_updated: 2026-11-04
tags: [design, dbt-spark, python, session, materializations]
---

# Introduction

This specification describes how the Spark adapter executes Python models in
the dbt process using a local Spark driver session. It defines submission,
generated-code execution, input loading, output conversion, persistence, and
error behavior, with explicit limitations. It records current behavior rather
than authorizing implementation of the improvements identified here.

## 1. Purpose & Scope

The audience is adapter maintainers, implementation agents, operators, and test
authors. Scope includes:

- The profile connection setting `method: session`.
- The independent model setting
  `submission_method: spark_session_based_cluster`.
- Python `table` and `incremental` materializations, including bounded
  microbatch execution and the session-specific temporary-view staging path.
- The same submission helper's use by `streaming`, whose streaming-specific
  contract remains in the existing streaming specification.

Remote notebook/job submission is excluded except where it explains differences
from the local helper. Python views, native Python schema-contract enforcement,
automatic dependency installation, transactional execution, and complete
streaming lifecycle management are not introduced by this specification.

The supported operating assumption is that SQL operations and Python model
execution use the same live Spark session and compatible catalog configuration.
The implementation relies on Spark session reuse; it does not verify that
assumption at every submission.

## 2. Definitions

| Term | Definition |
| --- | --- |
| dbt | The model compilation and execution framework using this adapter. |
| Profile connection method | The `method` credential controlling how SQL and metadata operations reach Spark. |
| Python submission method | Model configuration selecting a registered Python execution helper. |
| Session helper | `SparkSessionBasedClusterPythonJobHelper`, registered as `spark_session_based_cluster`. |
| SparkSession | Spark's entry point for SQL and DataFrame operations. A local Python driver can use distributed executors. |
| Compiled code | Python model code plus generated dbt reference/configuration support and materialization code. |
| DataFrame | A Spark or supported pandas-family tabular result returned by `model(dbt, spark)`. |
| WriterV2 | Spark DataFrameWriterV2, used to create/append/replace physical tables on the applicable paths. |
| Temporary view | Session-scoped Spark view exposing a DataFrame without creating a physical staging table. |
| Microbatch | dbt-core incremental processing split into bounded batches; not a Structured Streaming query. |
| Iceberg | Apache Iceberg, the default physical table format in this adapter. |
| UTC | Coordinated Universal Time. |
| ADR | Architecture decision record; may describe planned work, not implemented support. |

## 3. Requirements, Constraints & Guidelines

### Current execution requirements

- **REQ-001**: Register the session helper under
  `spark_session_based_cluster`. Resolve it through the base adapter's
  `submit_python_job()` dispatch using the model's `submission_method`.
  The adapter default remains `all_purpose_cluster`, not the session helper.
- **REQ-002**: For a profile with `method: session`, create a session SQL
  connection wrapper. SQL cursor execution shall obtain Spark through
  `get_spark_session(server_side_parameters)`, which applies configured builder
  parameters before calling `getOrCreate()`.
- **REQ-003**: The session Python helper shall execute synchronously in the
  current Python process. It shall obtain Spark using
  `SparkSession.builder.getOrCreate()` and shall not submit a remote job.
  Its constructor stores the parsed model but does not apply the supplied
  credentials to the builder.
- **REQ-004**: Before execution, deep-copy the parsed model configuration into
  `model_config`, import `datetime` and `tzlocal`, and perform these literal
  replacements throughout the compiled source:
  - `<UTC>` becomes `datetime.timezone.utc`.
  - `tzlocal()` becomes `tzlocal.get_localzone()`.
- **REQ-005**: Execute the resulting source with `exec()` using a new dictionary
  containing `spark`, `datetime`, and `tzlocal`, then updated with the helper's
  `locals()`. Use one dictionary for execution globals/locals so generated
  functions can access generated configuration and reference mappings.
- **REQ-006**: Generated batch materialization code shall construct
  `dbtObj(load_df_from_table_or_sql)` and call `model(dbt, spark)`.
  Table-valued inputs shall load with `spark.table()`; inputs classified as SQL
  by the current heuristic shall load with `spark.sql()`.
- **REQ-007**: Accept a Spark DataFrame directly. If available, convert pandas,
  pandas-on-Spark, or Koalas DataFrames to Spark using the existing conversion
  order. Reject other result types explicitly before publishing their output.
- **REQ-008**: After model evaluation, set `PYARROW_IGNORE_TIMEZONE=1` if that
  environment variable is absent. Preserve an existing value. This is a
  process-wide change, not isolated to one model.
- **REQ-009**: For an existing incremental target, the session submission
  method shall stage model output with `createOrReplaceTempView()` and consume
  that view through the incremental SQL strategy in the same session.
- **REQ-010**: For physical batch writes on the ordinary, non-write-order path,
  use `df.writeTo(target).using(file_format)` with default format `iceberg`,
  `.option("overwriteSchema", "true")`, supported partition expressions,
  location configuration, Iceberg table properties, and `.create()`.
- **REQ-011**: Preserve the separate managed write-order path when enabled:
  it delegates creation, append/replacement, and order synchronization to the
  existing write-order manager. Do not equate that path with ordinary `.create()`.
- **REQ-012**: On an exception derived from `Exception` during helper execution,
  print the formatted traceback and raise
  `DbtRuntimeError("The Python model failed: <error>")` with the original error
  as its cause. Successful helper submission returns `None`; the adapter
  generates the normal `OK` response rather than collecting the model's rows.

### Constraints and limitations

- **CON-001**: Profile `method: session` does not automatically select Python
  session submission. Conversely, selecting the session helper does not validate
  that SQL uses a session connection. Mixing it with remote SQL can leave data
  and temporary views inaccessible to subsequent SQL operations.
- **CON-002**: Unlike SQL session acquisition, the Python helper does not call
  `get_spark_session()` or apply `server_side_parameters`. Existing SQL/setup
  work normally establishes the configured session. Direct helper execution
  without that initialization may obtain an unconfigured session. No universal
  identity guarantee across threads or explicitly created Spark sessions is made.
- **CON-003**: The helper supplies no per-model interpreter/process isolation,
  package installation, Databricks library provisioning, job polling, retry,
  submission timeout, job handle, or Spark job-group cancellation.
  Remote helper configuration such as `packages`, `additional_libs`, and
  `timeout` has no corresponding provisioning/enforcement in this helper.
- **CON-004**: Session wrapper `cancel()` and `rollback()` only log that they
  are unimplemented. Closing a cursor clears cursor results; it does not stop
  Spark, remove temporary views, or roll back model effects.
- **CON-005**: The temporary-view staging name uses the target identifier plus
  `__dbt_tmp` and, when available, `_<model.batch.id>`. Database/schema inclusion
  is removed for session temporary views. Batch suffixes distinguish batches
  within a target; they do not provide cross-project or cross-invocation isolation.
- **CON-006**: Session incremental temporary views are not dropped by the
  materialization's physical-staging cleanup. They remain until replaced,
  explicitly dropped, or their Spark session ends. Publishing a view does not
  cache the underlying DataFrame or guarantee exactly-once computation.
- **CON-007**: A clean execution dictionary does not isolate imported modules,
  environment variables, Spark settings, cached data, filesystem effects, or
  active streams. `locals()` also exposes helper implementation variables;
  those names are not a supported model API.
- **CON-008**: Source replacements are textual, not syntax-aware. They may also
  change string literals, comments, or user functions containing the same text.
  They repair known generated timezone representations but do not validate or
  correctly serialize every possible Python configuration value.
- **CON-009**: The batch loader's SQL heuristic is: stripped text starts with
  `(` and ends with `)`, or the text contains `"select "` case-insensitively.
  It is not a SQL parser. A table identifier containing that substring can be
  misclassified; arbitrary query spellings are not uniformly recognized.
- **CON-010**: Native Python schema-contract enforcement is not implemented by
  this path. The Python writer does not validate the returned schema against
  declared columns like the SQL contract path. The existing contracts ADR
  records a dbt-core prerequisite and planned adapter work, not released support.
- **CON-011**: Ordinary physical `.create()` is not a universal replacement
  operation despite `overwriteSchema=true`. Existing Iceberg/Delta targets can
  remain present on table/full-refresh branches, so unsupported replacement
  cases can fail with table-already-exists errors. Use the existing write-order
  specification for that separate path's replacement guarantees.
- **CON-012**: Python partition writers support identity columns and Iceberg
  `years`, `months`, `days`, `hours`, and `bucket` transforms. The validation
  macro rejects `truncate` for Python/streaming writers. SQL-only partition
  features must not be presented as Python support.
- **CON-013**: Streaming uses its own readers, requires a streaming Spark
  DataFrame, and bypasses batch pandas-family conversion. Batch table/incremental
  code is not an alternative streaming sink. Neither helper response nor
  non-waiting streaming startup proves that all source rows have been processed.
- **CON-014**: No common transaction encloses Python model side effects,
  Spark writes, hooks, and metadata operations. An error may leave earlier
  side effects or catalog changes intact. `KeyboardInterrupt` and `SystemExit`
  are outside the helper's `except Exception` handler.
- **SEC-001**: Model code shall be treated as trusted executable Python with
  the dbt process's permissions. This execution namespace is not a security
  sandbox; deployment storage and network permissions remain authoritative.
- **GUD-001**: Prefer Spark DataFrame output when possible, preinstall required
  dependencies, and avoid model code that stops/replaces the shared Spark session.
- **GUD-002**: Changes to session acquisition, source serialization, cleanup,
  or dependency provisioning require their own explicit behavior contract and
  regression tests; do not silently broaden existing support.

## 4. Interfaces & Data Contracts

### Configuration

Both settings are required to deliberately select local session execution:

```yaml
# profiles.yml: credentials under a selected output
example:
  target: local
  outputs:
    local:
      type: spark
      method: session
      host: localhost
      schema: analytics
      threads: 1
      server_side_parameters:
        spark.master: "local[2]"
        spark.app.name: dbt-python-session
```

```yaml
# dbt_project.yml
models:
  example:
    +materialized: table
    +submission_method: spark_session_based_cluster
    +file_format: parquet
    +location_root: /tmp/dbt-python-models
```

`host` remains required by credential validation even though it is not used as
a remote Python submission endpoint. Database credential normalization remains
the existing Spark behavior: omit `database` or set it equal to `schema`;
credentials then normalize it to null. Model-level catalog routing and Spark
catalog settings are separate. Credential construction stringifies
`server_side_parameters` and forces `spark.sql.ansi.enabled` to `"false"`;
this does not mean model code cannot subsequently change the live setting.

### Execution interfaces

| Interface | Contract |
| --- | --- |
| `submit_python_job(parsed_model, compiled_code)` | Select helper by model configuration, submit code, then produce adapter response. |
| `SparkSessionBasedClusterPythonJobHelper(parsed_model, credential)` | Retain parsed model; credential is not applied by this constructor. |
| `helper.submit(compiled_code)` | Synchronous local execution, `None` on success, chained runtime error on handled failure. |
| `model(dbt, spark)` | Return a supported batch DataFrame; use `dbt.is_incremental` to branch according to compiled materialization context. |
| `dbt.ref()` / `dbt.source()` | Resolve compiled reference dictionaries and invoke the injected loader; not dynamic arbitrary dbt graph lookup. |
| `dbt.config.get()` | Read the generated configuration dictionary; runtime `dbt.config(...)` is not a general mechanism to change already-compiled materialization branches. |

### Output conversion order

1. If a pandas DataFrame and pandas-on-Spark are available, first construct a
   pandas-on-Spark DataFrame.
2. Otherwise, if a pandas DataFrame and Koalas are available, construct a Koalas
   DataFrame.
3. Keep a Spark DataFrame unchanged; convert pandas-on-Spark/Koalas with
   `.to_spark()`; otherwise convert pandas with `spark.createDataFrame()`.
4. Raise for unsupported results, including `None`, dictionaries, lists, and
   scalar values. An empty Spark DataFrame with an explicit schema is a valid
   return type, subject to provider constraints.

Optional conversion imports catch `ImportError`, not every initialization
failure. Conversion preserves the existing runtime behavior; no universal
timezone, precision, nullability, or pandas-version compatibility guarantee is
made.

### Publication surfaces

| Model path | Publication and lifetime |
| --- | --- |
| New batch table / new incremental target | Physical writer path; provider creation rules apply. |
| Existing incremental target, session helper | Unqualified temporary view, followed by SQL append/merge/overwrite strategy. |
| Existing incremental target, other helper | Physical staging table, cleaned after successful strategy execution. |
| Managed write-order model | Separate manager's create/replace/append contract. |
| Streaming | Persistent query with stable checkpoint; see streaming specification. |

Microbatch strategy remains Iceberg-only and requires non-empty partitioning.
Per-batch names and once-per-target metadata synchronization do not establish
isolation between independently concurrent dbt invocations.

## 5. Acceptance Criteria

- **AC-001 (REQ-001, CON-001)**: Given a session profile without a submission
  override, when Python dispatch runs, then it retains the
  `all_purpose_cluster` default; given the explicit override, the session helper
  is selected.
- **AC-002 (REQ-002, REQ-003, CON-002)**: Given configured server parameters,
  when the SQL cursor obtains Spark, then it applies them; when the Python
  helper obtains Spark, then it calls builder `getOrCreate()` without applying
  those credential parameters itself.
- **AC-003 (REQ-004, REQ-005, CON-008)**: Given compiled UTC/local-timezone
  representations and generated functions using globals, when submitted, then
  the exact substitutions occur and the functions can access injected names.
  Identical text inside a string literal is also changed by current behavior.
- **AC-004 (REQ-006, CON-009)**: Given a table name or a parenthesized/select
  SQL input, when the batch loader runs, then it calls the API chosen by the
  documented heuristic, including its known misclassification boundaries.
- **AC-005 (REQ-007)**: Given each supported DataFrame family, when its optional
  dependency is available, then conversion follows the documented order.
  Unsupported output raises rather than silently producing an empty table.
- **AC-006 (REQ-008)**: Given absent or preconfigured
  `PYARROW_IGNORE_TIMEZONE`, when batch materialization runs, then it sets only
  the absent value and does so after calling the user model.
- **AC-007 (REQ-009, CON-005, CON-006)**: Given an existing incremental target
  and session helper, when the model runs, then its view is visible to the
  strategy in the same session, no physical staging location is created, and
  the view remains after successful materialization.
- **AC-008 (REQ-010, REQ-011, CON-011)**: Given physical publication, when
  generated code is inspected/executed, then ordinary creation and managed
  write-order replacement use their distinct documented APIs. The ordinary
  path must not be described as guaranteed replace-or-create.
- **AC-009 (REQ-012, CON-014)**: Given a model exception, when the helper handles
  it, then traceback output and the chained runtime error retain the cause,
  with no automatic retry or rollback.
- **AC-010 (REQ-012, CON-003, CON-004)**: Given successful submission, when the
  adapter creates its response, then it returns `OK` without a row-count/job
  result contract; closing the SQL cursor does not stop Spark.
- **AC-011 (CON-010, CON-012, CON-013)**: Given contract enforcement, Python
  `truncate` partitioning, or streaming-specific output, when assessing support,
  then the documented restrictions apply rather than inheriting SQL/batch support.

## 6. Test Automation Strategy

- **Test levels and frameworks**:
  - Use existing pytest/unittest, mocks, and Jinja rendering for dispatch,
    session acquisition, generated code, conversion, and failure propagation.
  - Use local Spark functional tests for a real dbt incremental run followed by
    an update, checking exact inserted/updated rows, permanent location, and
    temporary-view visibility.
  - Use the dedicated write-order and streaming suites only for their separate
    runtime contracts; rendered-code tests alone do not certify integration.
- **Test data management**: Use unique target identifiers and isolated
  warehouses. Restore environment/settings changed by tests; drop test views
  and relations explicitly. Never stop a shared developer Spark session merely
  to clean up a unit test.
- **CI/CD integration**: Use existing adapter runners. Live local functional
  tests require the configured Spark/Iceberg profile and catalog service.
  No new CI service is required by this documentation.
- **Coverage requirements**: Both settings and their independence; initialized
  versus direct helper execution; namespace and textual rewrites; each output
  family; temporary-view lifetime; creation/replacement boundaries; errors.
  No numeric coverage threshold is introduced.
- **Performance testing**: Assert no physical staging write on the session
  incremental path. Do not equate temporary views with caching or introduce a
  throughput guarantee without measurement.

Run related unit tests from `dbt-spark`:

```bash
hatch run unit-tests -- tests/unit/test_adapter.py tests/unit/test_session_correctness.py tests/unit/test_macros.py tests/unit/test_microbatch_macros.py tests/unit/test_streaming_materialization.py
```

The existing functional
`TestLocalTemporaryViewPythonStaging` case exercises the real session helper
through two dbt runs using the `local_spark_iceberg` profile. The alternative
physical-staging case substitutes a local helper under a different name; it
does not validate remote submission. Direct-helper timezone/namespace and
timeout/side-effect boundary tests should be added when modifying those paths;
the listed existing tests are not a claim of exhaustive coverage.

## 7. Rationale & Context

Running Python in the dbt process avoids remote notebook upload and preserves
access to session temporary views, enabling lightweight incremental staging.
The same design shares process and Spark state with subsequent SQL, which
explains its dependency, lifecycle, and isolation limitations.

Source substitutions accommodate generated microbatch datetime representations.
They are a narrow compatibility workaround, not a general serializer. A future
structured serialization change must preserve timezone behavior without
silently rewriting user literals.

The distinction between connection and submission settings is essential:
one chooses the SQL transport, while the other chooses where Python executes.
The word "local" describes the driver/submission process, not a requirement
that every Spark executor run locally.

## 8. Dependencies & External Integrations

### External Systems
- **EXT-001**: Spark runtime and configured catalogs/providers supporting the
  selected table writer and incremental SQL strategy.

### Third-Party Services
- **SVC-001**: No Databricks API or remote job service is used by this helper.
  Storage/catalog services may still be required by the deployment.

### Infrastructure Dependencies
- **INF-001**: Working Python/JVM Spark environment, driver connectivity to
  executors, and access to table storage/catalog metadata.
- **INF-002**: A consistently initialized, live Spark session for Python and
  SQL operations relying on shared temporary views.

### Data Dependencies
- **DAT-001**: Compiled dbt reference/configuration dictionaries and model output
  accepted by the selected provider.

### Technology Platform Dependencies
- **PLT-001**: PySpark and its compatible Java/runtime components. The project
  declares PySpark through the `session` extra; use its current declared bounds.
- **PLT-002**: `tzlocal` is imported unconditionally at submission time, even
  without microbatch. It is included in local test environments but is not
  explicitly declared in the project's `session` extra or base dependencies.
  A missing installation fails before model execution; no auto-install occurs.
- **PLT-003**: pandas, pandas-on-Spark, Koalas, and their conversion dependencies
  are optional and must be preinstalled when those outputs are used.
- **PLT-004**: `python_submissions.py` currently imports PySpark at module scope.
  The optional-import issue documented in the adapter correctness specification
  must not be mistaken for an already-completed fix.

### Compliance Dependencies
- **COM-001**: Deployment permissions govern trusted model code and storage.
  No interpreter sandbox, secret isolation, or new compliance guarantee is provided.

## 9. Examples & Edge Cases

### Batch model with deterministic Spark output

```python
def model(dbt, spark):
    dbt.config(materialized="incremental", incremental_strategy="merge", unique_key="id")
    return spark.sql("select 1 as id, 'updated' as value")
```

Configure both connection and submission settings from Section 4, using a
provider supporting merge, such as Iceberg with its required runtime/catalog.
On the second run, the session-specific path publishes an intermediate
temporary view before executing the merge.

### Operational edge cases

| Condition | Current behavior or limitation |
| --- | --- |
| `tzlocal` missing | Submission fails before calling `model()`. |
| Model returns `None` | Unsupported-output failure; no empty-result substitution. |
| Empty Spark DataFrame with schema | Type accepted; sink/provider constraints still apply. |
| Model calls `spark.stop()` | Shared session/view assumptions break; helper does not prevent or recover it. |
| Two invocations stage the same target identifier | They can replace the same temporary view; no invocation namespace is provided. |
| Remote SQL profile with session Python helper | No same-session validation; subsequent SQL may not see the local staging view. |
| Literal `"<UTC>"` in user model code | Text replacement changes it, even when it is ordinary string data. |
| Model hangs or starts background work | Helper has no timeout or background-work lifecycle controller. |
| Failure after user writes a table | Side effect remains unless explicitly handled elsewhere; no general rollback. |
| Existing ordinary physical target | `.create()` can fail; `overwriteSchema` is not a replace command. |

## 10. Validation Criteria

- Trace the source from base Python dispatch through the session helper to
  generated `py_write_table` code and incremental strategy execution.
- Verify separate session-acquisition paths and avoid implying credentials are
  applied by the Python helper.
- Preserve the output conversion order, error cause, response shape, and
  temporary-view lifetime documented here.
- Check feature claims against the existing streaming, write-order, staging,
  flag, and Python contracts documents; planned ADR work is not current support.
- Distinguish source-verified behavior, existing automated coverage, and required
  future tests. Creating this specification does not certify a live Spark run.
- Any intentional fix to a documented limitation must update this contract,
  related user documentation, and directly relevant tests.

## 11. Related Specifications / Further Reading

- [Base Python submission dispatch](../../dbt-adapters/src/dbt/adapters/base/impl.py)
- [Generated dbt Python object and reference helpers](../../dbt-adapters/src/dbt/include/global_project/macros/python_model/python.sql)
- [Spark Python submission helper](../src/dbt/adapters/spark/python_submissions.py)
- [SQL session integration](../src/dbt/adapters/spark/session.py)
- [Credential/session connection handling](../src/dbt/adapters/spark/connections.py)
- [Batch Python writer](../src/dbt/include/spark/macros/materializations/table.sql)
- [Incremental materialization](../src/dbt/include/spark/macros/materializations/incremental/incremental.sql)
- [Partition validation](../src/dbt/include/spark/macros/materializations/incremental/validate.sql)
- [Functional Python staging tests](../tests/functional/adapter/test_python_incremental_staging_location.py)
- [Python staging-location specification](spec-design-spark-python-incremental-staging-location.md)
- [Streaming model specification](spec-design-spark-streaming-models.md)
- [Iceberg write-order specification](spec-design-iceberg-write-order.md)
- [Required-location flag specification](spec-design-spark-flag-require-location-root.md)
- [Adapter correctness specification](spec-design-spark-adapter-correctness.md)
- [Python schema-contracts ADR](../docs/arch/adr-0002-spark-python-model-contracts.md)
