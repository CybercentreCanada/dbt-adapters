---
title: dbt-spark SQL and Python UDF Registration with Apache Polaris
version: "1.1"
date_created: 2026-11-05
last_updated: 2026-11-05
tags: [design, dbt-spark, spark, sql, python, udf, iceberg, polaris]
---

# Introduction

This specification records how the inspected dbt-databricks adapter builds SQL
and Python user-defined functions and defines corresponding support in
dbt-spark for open-source Apache Spark using Apache Polaris as an Apache Iceberg
REST catalog. The implementation registers session-scoped scalar
functions, not persistent Polaris function objects.

**Status:** implemented for Apache Spark 4.1.3, with native-parser, table-only
Polaris support. Live native dbt builds were verified against Polaris 1.8.0 and
Iceberg 1.12.0. The tests used dbt-core 1.14.0a1; minimum-core certification and
Databricks backend regression tests remain separate validation requirements.

## 1. Purpose & Scope

The audience is adapter maintainers, implementation agents, operators, and test
authors. The design covers:

- Native dbt function resources, their ordered arguments, declared return types,
  dependency ordering, and `{{ function('name') }}` references.
- SQL-bodied scalar functions on Apache Spark 4.x.
- Row-at-a-time Python scalar functions registered through PySpark.
- The profile connection setting `method: session`.
- Consumption by SQL models and by Python models explicitly configured with
  `submission_method: spark_session_based_cluster`.
- Reading and writing Iceberg tables through a Polaris REST catalog while
  evaluating functions in Spark.

Persistent UDF deployment, remote SQL connections, Databricks job/notebook
submission, Spark Connect, aggregate functions, table functions, JavaScript,
JVM artifact deployment, vectorized Python functions, and streaming consumers
are outside the first implementation. Existing behavior on these surfaces must
not change.

Assumptions: project code is trusted; the driver and executors have compatible
Python environments; the selected dbt-core runtime supports function resources.
A Spark driver running in the dbt process can still use distributed executors.
Polaris provides catalog services, not a Python execution environment.

## 2. Definitions

| Term | Definition |
| --- | --- |
| UDF | User-defined function; here, a function returning one scalar value per invocation. |
| SQL | Structured Query Language. |
| DDL | Data Definition Language, including `CREATE FUNCTION`. |
| REST | Representational State Transfer, the HTTP-based catalog interface. |
| DAG | Directed acyclic graph of dbt resources and their dependencies. |
| Function resource | A dbt resource with `resource_type: function`, implementation source, argument metadata, and return metadata. |
| Function relation | dbt's representation of a function name, with relation type `function`; not a table. |
| Registration | Making a function callable in a particular Spark session. |
| Deployment | Persisting a function definition for use independently of the creating session. Registration does not necessarily constitute deployment. |
| Session | A particular live `SparkSession`; two sessions sharing a `SparkContext` do not necessarily share temporary functions. |
| Session catalog | Spark's session-level catalog and function registry, distinct from an Iceberg REST catalog. |
| Polaris catalog | An Apache Polaris service exposed to Spark through the Iceberg REST catalog client. |
| Unity Catalog | Databricks' governance and catalog service; not interchangeable with Polaris. |
| JVM | Java Virtual Machine, used by Spark and JVM-backed external UDFs. |
| FunctionCatalog | Spark's catalog interface for resolving functions; its inspected Spark 4.0 API does not define function creation or replacement. |
| DDL type string | A Spark type expression such as `INT`, `DECIMAL(12,2)`, or `ARRAY<STRING>`. |
| Invocation registry | Adapter-owned, in-memory definitions successfully built during the current dbt invocation, available for registration in consuming sessions. |

## 3. Requirements, Constraints & Guidelines

### Requirements

- **REQ-001**: Use dbt function resources and the existing DAG. A selected
  function shall build before a dependent selected model. Do not treat a Python
  UDF as a Python model returning a DataFrame.
- **REQ-002**: The initial feature shall be opt-in for each function through
  `config.spark_function_scope: session`. An absent or different scope shall
  fail explicitly on open-source dbt-spark rather than imply persistence.
- **REQ-003**: SQL registration shall produce
  `CREATE OR REPLACE TEMPORARY FUNCTION`, an unqualified quoted name, ordered
  typed parameters, an explicit scalar return type, `LANGUAGE SQL`, and
  `RETURN <compiled body>`. Both a scalar expression and a scalar query body
  shall be supported according to Spark's SQL-function rules.
- **REQ-004**: Python registration shall construct a callable from the compiled
  function body and register it using `spark.udf.register`. The return type
  shall always be explicit; PySpark's implicit string return type is forbidden.
  Never submit Databricks `LANGUAGE PYTHON AS $$ ... $$` to Apache Spark.
- **REQ-005**: Python argument order shall match `model.arguments`. Validate
  Python parameter names, uniqueness, callable arity, and the return type before
  registration. Argument metadata shall not be advertised as warehouse-style
  enforcement of Python input types.
- **REQ-006**: Require Python `runtime_version` and `entry_point`, consistent
  with the inspected base `FunctionConfig`. `runtime_version` shall be a
  `major.minor` string matching the driver interpreter; it does not provision
  an interpreter. `entry_point` shall name the generated wrapper callable and
  must be a valid, non-keyword Python identifier. Executor compatibility shall
  be established by the distributed test.
- **REQ-007**: Function creation and `function()` references shall render the
  same unqualified, backtick-quoted function identifier. Function relations
  shall exclude database and schema from rendering. Table and view rendering
  shall remain unchanged. Catalog configuration still routes table references.
- **REQ-008**: Register selected functions in every actual consuming session
  before execution, in dependency order. Reuse the configured
  `get_spark_session(server_side_parameters)` acquisition path for SQL, Python
  registration, and session Python model submission. Do not assume that
  `getOrCreate()` guarantees session identity across threads.
- **REQ-009**: Track successfully built definitions by dbt unique ID and source
  fingerprint in the invocation registry, and track registration by actual
  session identity. A new session shall receive those definitions before a
  dependent SQL statement or session Python model executes. Failed registration
  shall not mark a definition successfully registered.
- **REQ-010**: Repeated registration of an unchanged definition shall be
  idempotent. A changed definition shall replace the previous adapter-owned
  registration. SQL signature changes unsupported by Spark replacement shall
  fail explicitly; do not silently drop and recreate functions.
- **REQ-011**: Reject duplicate physical function names across selected
  packages/schemas, under the active Spark case-sensitivity rules, before
  replacing either definition. Do not overwrite built-in or pre-existing
  functions not owned by the current invocation.
- **REQ-012**: Native dbt parsing and compilation shall perform no registration
  and execute no Python body. Building shall register the function; actual
  row-level execution occurs only when Spark evaluates a consuming query.
- **REQ-013**: A successful function materialization shall retain normal dbt
  result reporting and return `{'relations': [target_relation]}`. Python
  registration shall not emit a dummy SQL statement or a table artifact.
- **REQ-014**: SQL `volatility: deterministic` shall map to `DETERMINISTIC`;
  `non-deterministic` shall map to `NOT DETERMINISTIC`. Python nondeterminism
  shall use a PySpark UDF marked with `asNondeterministic()` before registration.
  An absent value shall preserve the engine/API default. Reject `stable` and
  unknown values rather than emit PostgreSQL volatility keywords.
- **REQ-015**: Temporary functions shall not receive persistent grants or
  warehouse documentation DDL. Reject configured grants or enabled persistent
  function documentation before mutation. Project descriptions may remain in
  dbt artifacts without claiming they were persisted in Polaris.
- **REQ-016**: A consuming function reference absent from the current
  invocation registry shall fail with guidance to include its function resource
  in the build selection. Check registration availability before consumer
  execution, not during parsing/compilation, when selected functions may not yet
  be built. State deferral shall not be interpreted as deployment of a
  session-scoped function. No automatic loading of unselected project code or
  external-session functions is required.
- **REQ-017**: Ending the dbt invocation shall clear its definition registry.
  Drop only temporary functions owned by that invocation from sessions it used;
  never stop a caller-owned Spark session. Cleanup errors shall be reported
  without concealing an earlier build/query failure. A later invocation must
  rebuild its required functions.
- **REQ-018**: Backend, compilation, registration, dependency, and worker
  execution failures shall surface as repository-standard dbt errors with
  function identity and original exception chaining where available. No
  unsupported request may produce a success result.
- **REQ-019**: The tested SQL UDF configuration shall omit
  `IcebergSparkSessionExtensions`, whose Iceberg 1.12.0 routine-parameter parser
  throws `UnsupportedOperationException`. Reject incompatible SQL function
  registration explicitly; do not silently remove static Spark extensions.
- **REQ-020**: Polaris users of the native-parser configuration shall explicitly
  enable `flags.spark_table_only: true`. This flag defaults to false, excludes
  persistent views from metadata/catalog discovery, and rejects persistent view
  materializations. Table-only discovery uses v2 table listing regardless of
  `use_v2_relation_listing`; normal profiles preserve existing view discovery.

### Security requirements

- **SEC-001**: Treat Python UDF source as executable, trusted project code.
  Registration is not a sandbox. Syntax validation is not a security boundary.
- **SEC-002**: Credentials, REST authorization headers, profile objects, and
  full session configuration shall not be injected into callable globals or
  serialized into UDF closures. Error messages shall not expose catalog secrets.
- **SEC-003**: Quote SQL identifiers using existing adapter helpers. Construct
  Python wrappers structurally or with validated identifiers; never concatenate
  unchecked names into executable source. Retain legitimate source code as
  code, not as an escaped SQL string.

### Constraints

- **CON-001**: SQL-bodied functions require Spark 4.0 or later in the targeted
  4.x support range. Older Spark SQL external-function DDL refers to JVM classes
  and is not a substitute for SQL-body or Python-body support.
- **CON-002**: Initial registration and consumption require `method: session`.
  Remote Thrift, HTTP, and ODBC sessions cannot obtain a driver-local Python
  registration merely because a Python job succeeded elsewhere.
- **CON-003**: Session functions are not persistent Iceberg objects. They shall
  not appear as tables in Polaris, acquire Iceberg storage locations, survive
  process termination, or become callable from another engine automatically.
- **CON-004**: An Iceberg catalog implementing Spark `FunctionCatalog` is not
  evidence of arbitrary UDF deployment support. Built-in Iceberg transforms
  and `system` procedures are distinct from project-defined functions.
- **CON-005**: Reject unsupported aggregate/table/vectorized functions, argument
  defaults, overloads, remote execution, and streaming consumers for this
  feature. These exclusions shall not disable pre-existing adapter features.
- **CON-006**: Preserve Databricks' existing persistent UDF path. Spark dispatch
  overrides inherited by dbt-databricks shall delegate to existing behavior
  when the effective adapter is not `spark`; do not impose session scope,
  temporary names, or Python wrapper requirements on Databricks.
- **CON-007**: Do not raise dbt-spark's general dbt-core minimum solely for this
  opt-in feature. Document dbt-core 1.11+ as the native function-resource
  prerequisite and test the minimum supported function runtime. Older supported
  dbt-core runtimes must retain existing non-function behavior.
- **CON-008**: The current native dbt Python model API does not supply
  `dbt.function()` or parse it as a function dependency. Python consumers shall
  establish ordering through `dbt.ref()` to a SQL model that references the
  functions and call the registered names through Spark. Literal names alone
  do not establish DAG dependencies.
- **CON-009**: Native-parser Polaris mode does not support persistent views or
  extension-specific Iceberg SQL statements. Use a table-only namespace without
  conflicting view names. Temporary Python staging views remain supported.

### Guidelines

- **GUD-001**: Reuse base `FunctionConfig`, function materialization dispatch,
  existing quoting, logging, and errors. Keep session state separate from the
  table-relation cache.
- **GUD-002**: Use simple scalar SQL UDFs instead of Python when equivalent
  expressions are available. Python worker serialization and execution can add
  overhead; no universal performance advantage is promised.
- **PAT-001**: Keep validation and registration out of generic table lookup.
  Use adapter methods decorated with `@available` for calls made from Jinja.

## 4. Interfaces & Data Contracts

### Inspected Databricks registration path

The inspected nested dbt-databricks checkout is at commit
`2e81485830fc07bf36952069993c09da67506004`; the containing repository was inspected
at `f3b6a4985112ea0b08457479102c145adc96d77d`.

| Surface | Observed behavior |
| --- | --- |
| [Global function materialization](../../dbt-adapters/src/dbt/include/global_project/macros/materializations/functions/function.sql) | Builds a function relation, runs hooks, obtains function configuration, dispatches `<type>_function_<language>`, executes the build value, and returns the function relation. |
| [Base function configuration](../../dbt-adapters/src/dbt/adapters/base/relation.py) | Requires language/type and Python runtime/entry-point metadata; supplies the function macro name. |
| [Databricks relation](../../dbt-databricks/dbt/adapters/databricks/relation.py) | Supports relation type `function`; supplies placeholder runtime/entry-point values when absent for Python because Databricks does not use these fields in its DDL. |
| [Databricks scalar macros](../../dbt-databricks/dbt/include/databricks/macros/materializations/functions/scalar.sql) | SQL: persistent `CREATE OR REPLACE FUNCTION ... LANGUAGE SQL RETURN ...`. Python: persistent `CREATE OR REPLACE FUNCTION ... LANGUAGE PYTHON AS $$ ... $$`. Explicit runtime/entry-point settings generate warnings because they have no effect on Databricks. |
| [Global execution helper](../../dbt-adapters/src/dbt/include/global_project/macros/materializations/functions/helpers.sql) | Executes a SQL statement, applies grants/docs, and commits. These persistent-object assumptions require a Spark override. |
| [Databricks functional tests](../../dbt-databricks/tests/functional/adapter/functions/test_udfs.py) | Exercise SQL/Python build, invocation through `function()`, repeat builds, changed bodies, and multiple arguments; identify Unity Catalog as required. |

Databricks does **not** register these dbt Python UDFs by calling
`spark.udf.register` in the inspected path. Its backend executes Python-function
DDL. Copying that DDL into Apache Spark is not this implementation.

### Implemented dbt contract

| Field | Contract |
| --- | --- |
| `resource_type` | `function`, supplied by dbt-core. |
| `language` | `sql` or `python`, determined by the implementation resource. |
| `config.type` | `scalar`; use the core scalar default where provided. |
| `config.spark_function_scope` | Required explicit `session` for this feature. |
| `arguments` | Ordered sequence of unique names and valid Spark data types; no defaults in the first implementation. |
| `returns.data_type` | Required scalar Spark DDL type string. Complex scalar values such as arrays are allowed when Spark/PySpark support the declared type. |
| `compiled_code` | SQL expression/scalar query, or Python statements comprising the generated callable body. Not complete `CREATE FUNCTION` DDL. |
| Python `config.entry_point` | Required name of the generated wrapper. It is not a Databricks handler clause. |
| Python `config.runtime_version` | Required `major.minor` driver compatibility declaration; no runtime installation. |
| Identifier | The core-resolved function identifier, rendered without catalog/schema qualification and quoted by the adapter. |
| Database/schema metadata | May remain in dbt artifacts for project context, but shall not imply physical namespacing of temporary functions. |

The Python body is wrapped once in a callable with the configured entry-point
name and the ordered argument names. Imports, local helper definitions, and
explicit null handling may occur inside that body. Do not require
`model(dbt, spark)` or a top-level named handler. Return-value compatibility is
ultimately evaluated by Spark's workers.

### Implemented adapter dispatch and lifecycle

Reuse native function parsing and introduce a Spark function materialization
with Spark-specific dispatch. This deliberate deviation from the initial design
bypasses the global materialization's premature table-only cache lookup:

| Surface | Required integration |
| --- | --- |
| `spark__scalar_function_sql(target_relation)` | Validate the SQL contract and return temporary Spark SQL DDL; do not inherit the global dollar-quoted SQL body or `IMMUTABLE`/`STABLE`/`VOLATILE` rendering. |
| `spark__scalar_function_python(target_relation)` | Validate metadata and return a discriminated Python registration value, not SQL. |
| `spark__function_execute_build_sql(build_value, existing_relation, target_relation)` | On Spark, validate the value against the model, register it atomically, store the `main` adapter response, record successful definitions, and omit persistent grants/docs. For other effective adapters, retain the existing SQL execution helper. |
| `SparkRelation` function rendering/creation | Apply identifier-only inclusion and quoting exclusively to function relations, including relations constructed by core's runtime `function()` resolver. |
| Function configuration and relation lookup | Recognize `resource_type: function` even when the initial relation type is not yet set. Do not route function existence through Iceberg table discovery, `DESCRIBE TABLE`, or the table cache; use the invocation/session function registry. |
| Adapter registration method | `register_spark_function(model, build_value)` is an `@available` method accepting serialized function metadata and the matching SQL or Python build value. It returns `AdapterResponse(_message="OK")` after successful registration and raises on failure. |
| Session SQL cursor and Python session submission | Ensure the invocation's available function definitions are registered in the acquired session before consumers execute. Registration SQL must bypass this check to avoid recursion. |
| Invocation completion | Clear definitions and remove owned registrations without touching unrelated functions or stopping Spark. |

The Python build value has this logical schema:

```text
SparkPythonRegistration:
  kind: "spark_python_udf"
  unique_id: string
  identifier: string
  language: "python"
  arguments: ordered sequence[(name: string, data_type: string)]
  return_type: string
  body: string
  dependencies: ordered sequence[string]
  entry_point: string
  runtime_version: string
  volatility: null | "deterministic" | "non-deterministic"
```

The execution helper accepts `string | SparkPythonRegistration` on Spark.
Unknown discriminators, missing fields, and invalid types are errors. On
Databricks the existing dispatched Python macro still returns a SQL string and
must continue through its existing persistent execution path.

Registration state is keyed by `(invocation, session identity, unique_id,
fingerprint)`, with function-name ownership checked separately. Synchronize
concurrent registrations; publish state only after success. Registry replay
uses only successfully built definitions, preserves dependency ordering, and
never retries an arbitrary failed user query.

The global materialization's initial `load_cached_relation(this)` call must be
handled without table-only metadata queries for function resources. This is an
integration requirement, not permission to modify table lookup behavior.
Compile-time `function()` resolution must still establish DAG dependencies
before execution-time registry availability checks occur.

### Backend capability matrix

| Capability | Apache Spark + Polaris | Databricks reference |
| --- | --- | --- |
| SQL-body scalar UDF | Spark 4.x supports temporary SQL functions; implemented adapter path. | Persistent backend SQL function DDL in inspected adapter. |
| Python-body scalar UDF | Driver API registration; session-scoped; implemented adapter path. | Backend `LANGUAGE PYTHON` DDL with Unity Catalog. |
| Durable SQL UDF | Spark can persist functions in a suitable session/external metastore; this is not demonstrated Polaris deployment. | Supported reference backend path. |
| Durable Python UDF | No supported create/replace pipeline established for this Spark/Polaris combination. | Supported reference backend path. |
| Functions in another Spark session | Must register again. | Persistent backend objects may be resolved subject to backend permissions. |
| Cross-engine UDF invocation | Not provided by session registration. | Not a compatibility promise of this specification. |
| Iceberg transform functions/procedures | Runtime-provided functions and maintenance procedures; not an arbitrary UDF registry. | Separate from project UDF deployment. |
| Function grants/docs in Polaris | Not provided for session functions. | Existing persistent execution path applies backend grants/docs behavior. |

## 5. Acceptance Criteria

- **AC-001 (REQ-001, REQ-003, REQ-007)**: Given a selected SQL function and an
  Iceberg model referencing it, when `dbt build` executes, then registration
  precedes the model, invocation is identifier-only, and the model stores the
  expected scalar results.
- **AC-002 (REQ-004, REQ-005, REQ-006)**: Given a Python body with two arguments
  and compatible runtime metadata, when built and evaluated on distributed
  rows, then argument ordering and declared result type are preserved, including
  explicit `None` handling.
- **AC-003 (REQ-008, REQ-009)**: Given two distinct sessions or multiple dbt
  worker threads, when dependent consumers execute, then each actual session
  receives the required selected definitions before evaluating the consumer.
- **AC-004 (REQ-010)**: Given a built function, when its body is rebuilt in the
  invocation, then unchanged source is idempotent and changed source yields new
  results; unsupported SQL signature replacement reports an error.
- **AC-005 (REQ-002, CON-002, CON-005)**: Given absent session opt-in, remote
  execution, or an unsupported function kind, when build is attempted, then an
  actionable error occurs before function mutation.
- **AC-006 (REQ-011)**: Given duplicate temporary names, or a name already
  owned by Spark/user code, when registration is attempted, then neither
  existing definition is overwritten.
- **AC-007 (REQ-012, REQ-013)**: Given valid source, when parsing/compiling,
  then no function is registered; when building, then dbt reports a function
  result without dummy SQL or a table result.
- **AC-008 (REQ-014, REQ-015)**: Given supported volatility, when registering,
  then correct Spark semantics are used; given `stable`, grants, or persistent
  docs settings, then validation fails before mutation.
- **AC-009 (REQ-016, REQ-017)**: Given a model-only selection, a deferred
  function, or a later invocation without rebuilding the function, when a
  function is referenced, then there is no false claim of deployed availability.
  Invocation cleanup removes only owned temporary functions.
- **AC-010 (REQ-018, SEC-001, SEC-002, SEC-003)**: Given invalid types/names,
  driver syntax errors, or worker exceptions, when build/evaluation occurs,
  then explicit contextual errors surface without exposing credentials or
  turning failures into success.
- **AC-011 (CON-001, CON-003, CON-004)**: Given a Polaris Iceberg table and a
  session UDF, when Spark evaluates and stores results, then no function
  deployment is recorded in Polaris; a new external session does not inherit
  the temporary UDF.
- **AC-012 (CON-006, CON-007)**: Given existing Databricks UDF projects or older
  supported dbt-core runtimes running non-function Spark projects, when their
  existing suites run, then no session-only behavior leaks into those paths.
- **AC-013 (REQ-019, REQ-020, CON-009)**: Given Polaris, Spark 4.1.3, the
  native parser, and table-only opt-in, when a SQL/Python UDF build runs, then
  table discovery and both SQL/Python table consumers succeed without `SHOW
  VIEWS`. Given the incompatible Iceberg parser or a persistent view
  materialization in table-only mode, validation fails explicitly.

## 6. Test Automation Strategy

- **Test Levels**: Unit tests for validation/dispatch/rendering and state;
  live Spark tests for registration and session isolation; functional dbt tests
  for DAG integration; end-to-end tests against actual Polaris for Iceberg
  catalog coexistence.
- **Frameworks**: Existing pytest, Jinja macro fixtures, dbt adapter functional
  fixtures, and the repository's Hatch environments. Reuse shared UDF fixtures
  where appropriate, but do not inherit persistent-lifecycle expectations
  unchanged.
- **Test Data Management**: Use unique per-test function names and Polaris
  namespaces. Seed integer, decimal, string, and null inputs; include zero and
  multiple arguments. Clean owned functions and test tables in finalizers.
  Provision credentials through the existing test environment mechanism only.
- **CI/CD Integration**: Run targeted unit tests through
  `hatch run unit-tests -- <test selectors>` from the adapter directory.
  Add live tests to the existing local Spark environment and Polaris functional
  profile/job; a filesystem-only Iceberg catalog does not satisfy the Polaris
  acceptance gate. Run `hatch run code-quality` for implementation changes.
- **Coverage Requirements**: Cover every acceptance criterion, including all
  rejection paths. No new numeric coverage threshold is introduced.
- **Runtime Matrix**: Test the minimum feature-capable dbt-core release and
  the maintained current runtime; test Spark 4.0 and the maintained 4.x runtime
  with compatible Iceberg integrations. Record Polaris/Iceberg versions in
  test evidence. Test non-function compatibility separately on older supported
  dbt-core.
- **Performance Testing**: Verify that unchanged definitions register at most
  once per session/fingerprint, not once per row or statement. Use multi-partition
  queries to force Python worker execution. Benchmark SQL and Python variants
  for operational guidance; no latency or throughput guarantee is specified.
- **Failure/Concurrency Testing**: Exercise duplicate names, failed replay,
  session replacement, concurrent workers, failed cleanup, unsupported SQL
  signature changes, and missing selected dependencies.

## 7. Rationale & Context

### Implementation validation and approved compatibility scope

Live native dbt integration tests now verify a four-thread build of SQL and
Python functions, a SQL table consumer, and a session Python table consumer;
parsing/compilation without registration; invocation cleanup; model-only
selection errors; and rebuilding selected function ancestors. The functional
fixtures also verify a custom core-resolved function alias and successful
function adapter-response shapes.

The same workflow passed against isolated Polaris 1.8.0, Iceberg 1.12.0, and
S3-compatible storage on Spark 4.1.3. Native-parser mode and explicit table-only
discovery were required; both consumer tables were verified to use the Iceberg
provider in the Polaris catalog. With the Iceberg extension enabled, the JVM trace
failed at `IcebergSparkSqlExtensionsParser.parseRoutineParam`; without it,
unmodified `SHOW VIEWS` discovery failed with `MISSING_CATALOG_ABILITY.VIEWS`.
The user explicitly selected native-parser, table-only discovery rather than
adding a JVM parser compatibility extension. Neither default metadata behavior
nor installed Spark extensions are silently modified.

Temporary SQL function signature replacement was accepted by the tested Spark
4.1.3 runtime. The adapter delegates supported replacement semantics to Spark,
without maintaining simultaneous overloads or dropping functions to force a
replacement.

### Initial feasibility evidence

The inspected dbt-spark package declares PySpark 4.x for session execution and
already exposes the session connection wrapper and a local Python model helper.
Its SQL cursor and Python session model helper now use configured
`get_spark_session`; the adapter passes the exact registered session to Python
UDF consumers. The original Python helper used an unconfigured
`SparkSession.builder.getOrCreate()`.

A local experiment on 2026-11-05 used the existing environment with PySpark
4.1.3 and dbt-core 1.14.0a1. Results:

| Check | Observed result |
| --- | --- |
| Temporary SQL function doubling `3` | `6`. |
| `spark.udf.register` Python function doubling `3` | `6`. |
| Explicit Python null handling | SQL null result. |
| Lookup from `spark.newSession()` | Neither temporary SQL nor Python function exists there. |
| Catalog-qualified invocation of the temporary Python function | `AnalysisException`. |
| Databricks-style Python-function DDL | `ParseException`. |
| Core runtime `function()` resolver | Creates an adapter relation with `RelationType.Function`; function-specific relation rendering is therefore necessary. |

The initial experiment did not run Polaris or a complete dbt materialization.
Subsequent integration tests described above cover those paths. The minimum
supported feature-core runtime is still not independently certified.

### Polaris and Iceberg limitations

Polaris governs catalog access and Iceberg assets; Spark evaluates the UDF and
accesses table data with the configured engine credentials. A UDF's executable
code, dependencies, runtime selection, and session lifetime are not supplied by
the table catalog. Catalog permissions are not an isolated UDF execution sandbox.

The released Iceberg 1.10.0 REST API inspected here has no function endpoints.
The newer Iceberg development API at commit
`b59c059430d334d6a381c484fb73bb5094d00741` contains **GET list/load function**
endpoints and function metadata definitions, but no corresponding function
create/replace/delete endpoints in the inspected function routes. Therefore,
"Iceberg REST never supports functions" would be inaccurate. Read-only metadata
routes alone still do not establish a deployable SQL/Python UDF lifecycle.

The inspected Spark 4.0 `FunctionCatalog` API exposes list/load/existence.
The inspected Iceberg Spark integration resolves runtime-provided functions
through `SparkFunctions`, including its `system` namespace, rather than
implementing arbitrary project-function creation. Polaris' Spark guide describes
the Iceberg REST integration, not a verified UDF deployment path.

Future persistent support requires independently verifying the exact Polaris
server, REST client, Spark catalog implementation, creation/update APIs, stored
representations, and engine execution behavior. Do not infer support from newer
metadata schemas, generic catalog extensions, or successful table registration.

Session-only support is consequently an explicit compatibility subset of dbt's
usual warehouse-object UDF model. It cannot support a separate `dbt build
--select resource_type:function` command followed by a new process consuming
those functions. The normal supported workflow builds functions and consumers
together.

## 8. Dependencies & External Integrations

### External Systems

- **EXT-001**: Apache Spark - parses SQL UDF definitions, hosts session
  registries, and evaluates SQL/Python function calls.
- **EXT-002**: Apache Polaris - supplies authenticated Iceberg REST catalog
  operations and asset authorization for test/model tables; persistent UDF
  deployment is not assumed.

### Third-Party Services

- **SVC-001**: No additional hosted UDF service is required. Databricks/Unity
  Catalog is reference behavior, not a dependency of the Spark implementation.

### Infrastructure Dependencies

- **INF-001**: Spark driver and executors with compatible Python interpreters
  and pre-provisioned function dependencies; packages are not installed from
  UDF configuration.
- **INF-002**: Polaris-compatible object storage and Iceberg runtime libraries
  matching the tested Spark major/minor integration.
- **INF-003**: A live Polaris integration-test environment. Local Spark-only
  tests cannot validate authentication and catalog routing.

### Data Dependencies

- **DAT-001**: dbt function implementation files and function property metadata,
  available during build with selected dependencies.
- **DAT-002**: Small synthetic Iceberg input tables containing representative
  values and nulls; production or personal data is unnecessary.

### Technology Platform Dependencies

- **PLT-001**: Feature-capable dbt-core 1.11+ with function resources and
  `function()` resolution. Keep the general adapter compatibility range intact.
- **PLT-002**: Apache Spark 4.x for the SQL-body implementation;
  compatible PySpark and Python worker runtimes for Python execution. The
  implemented target is Spark 4.1.3 with Iceberg 1.12.0's native-parser
  compatibility restrictions above.
- **PLT-003**: Base dbt-adapters function contracts/materialization dispatch
  and the existing Spark session connection architecture.

### Compliance Dependencies

- **COM-001**: Existing repository credential-handling and trusted-code
  requirements. No new regulatory certification or UDF security isolation is
  claimed.

## 9. Examples & Edge Cases

### SQL function and Iceberg consumer

Function properties:

```yaml
functions:
  - name: double_amount
    config:
      type: scalar
      spark_function_scope: session
      volatility: deterministic
    arguments:
      - name: amount
        data_type: INT
    returns:
      data_type: INT
```

Function SQL source:

```sql
amount * 2
```

Expected registration:

```sql
CREATE OR REPLACE TEMPORARY FUNCTION `double_amount`(amount INT)
RETURNS INT
LANGUAGE SQL
DETERMINISTIC
RETURN amount * 2;
```

Model source:

```sql
{{ config(materialized='table', file_format='iceberg') }}
select {{ function('double_amount') }}(amount) as doubled_amount
from {{ ref('amounts') }}
```

Build the function and model together, for example
`dbt build --select +doubled_amounts`, where the model name is `doubled_amounts`.
The rendered function reference is `` `double_amount` ``, not
`` `polaris`.`analytics`.`double_amount` ``.

### Python body

Properties for a driver running Python 3.12:

```yaml
functions:
  - name: multiply_amount
    config:
      type: scalar
      spark_function_scope: session
      entry_point: multiply_amount_impl
      runtime_version: "3.12"
    arguments:
      - name: amount
        data_type: INT
      - name: multiplier
        data_type: INT
    returns:
      data_type: INT
```

Function Python source:

```python
if amount is None or multiplier is None:
    return None
return amount * multiplier
```

Equivalent registration shape, shown for illustration rather than adapter code:

```python
def multiply_amount_impl(amount, multiplier):
    if amount is None or multiplier is None:
        return None
    return amount * multiplier

spark.udf.register("multiply_amount", multiply_amount_impl, "INT")
```

### Edge cases

| Case | Required outcome |
| --- | --- |
| A new session shares the same SparkContext | Re-register available selected definitions; do not assume shared temporary functions. |
| Same alias in two dbt schemas/packages | Reject the collision; temporary functions have no independent schema scope. |
| Driver runtime differs from `runtime_version` | Fail validation; do not silently substitute an interpreter. |
| Missing library on executors | Surface the worker error; do not install packages automatically. |
| Python body references driver `spark` or dbt/profile state | Not a supported closure contract; do not inject those objects. |
| SQL scalar body contains unsupported aggregate/window expressions | Surface Spark validation errors; do not transform it into a different function kind. |
| Model-only command after a prior build exits | Require rebuilding the function in the new invocation. |
| Persistent grants, persistent docs, or `--defer` availability | Do not claim deployment; reject unsupported settings/references. |
| SQL and Python function use the same physical name | Reject; ownership is independent of implementation language. |
| Registration fails during replay | Dependent execution fails explicitly; no stale success marker. |

## 10. Validation Criteria

Implementation conforms only when:

1. AC-001 through AC-013 pass with traceable test evidence.
2. A real Polaris-backed Iceberg model consumes both SQL and Python UDFs during
   one dbt build, including multiple dbt threads and Python worker evaluation.
3. Parsing, compilation, function-name rendering, and function result artifacts
   are checked through dbt-core, not only mocked Jinja or raw Spark calls.
4. Session isolation, replay, ownership, cleanup, and absent-function errors
   satisfy the documented lifetime contract.
5. Databricks SQL/Python UDF lifecycle tests remain unchanged and pass;
   non-function Spark behavior remains compatible.
6. Tests establish the minimum feature runtime, rather than treating the local
   prerelease core experiment as minimum-version certification.
7. User documentation clearly distinguishes session registration from persistent
   warehouse deployment and lists unsupported execution methods.
8. Implementation changes include a user-facing changelog entry and pass
   targeted tests and repository code quality. Documentation creation alone
   does not require adapter builds or credentialed integration tests.
9. Any future persistent Polaris UDF claim is supported by a separate updated
   specification and tested end-to-end deployment/invocation lifecycle.

## 11. Related Specifications / Further Reading

### Repository references

- [Session-based Python model execution](spec-design-spark-session-python-model-execution.md)
  records current session acquisition and its limitations; this design proposes
  additional requirements for UDF consumers.
- [Python incremental staging locations](spec-design-spark-python-incremental-staging-location.md)
  addresses table staging, not function storage.
- [Spark session connection](../src/dbt/adapters/spark/session.py)
- [Spark Python submission helpers](../src/dbt/adapters/spark/python_submissions.py)
- [Spark relation rendering](../src/dbt/adapters/spark/relation.py)
- [Session function registry](../src/dbt/adapters/spark/functions.py)
- [Spark function materialization](../src/dbt/include/spark/macros/materializations/functions.sql)
- [Native dbt/Polaris function tests](../tests/functional/adapter/test_functions.py)
- [Shared UDF tests](../../dbt-tests-adapter/src/dbt/tests/adapter/functions/test_udfs.py)
- [Databricks scalar macro unit tests](../../dbt-databricks/tests/unit/macros/materializations/functions/test_scalar_functions.py)

### Primary external sources

Sources were consulted on 2026-11-05. Versioned/revision-pinned links delimit the
claims; development specifications are not evidence of released runtime support.

- [dbt native UDF prerequisites and project workflow](https://docs.getdbt.com/docs/build/udfs)
- [Spark 4.0.1 SQL-bodied CREATE FUNCTION](https://spark.apache.org/docs/4.0.1/sql-ref-syntax-ddl-create-sql-function.html)
- [Spark 4.0.1 external JVM CREATE FUNCTION](https://spark.apache.org/docs/4.0.1/sql-ref-syntax-ddl-create-function.html)
- [PySpark 4.0.1 UDFRegistration.register](https://spark.apache.org/docs/4.0.1/api/python/reference/pyspark.sql/api/pyspark.sql.UDFRegistration.register.html)
- [Spark 4.0.1 FunctionCatalog API](https://spark.apache.org/docs/4.0.1/api/java/org/apache/spark/sql/connector/catalog/FunctionCatalog.html)
- [Polaris Spark integration guide](https://polaris.apache.org/guides/spark/)
- [Released Iceberg 1.10.0 REST API](https://github.com/apache/iceberg/blob/apache-iceberg-1.10.0/open-api/rest-catalog-open-api.yaml)
- [Inspected development Iceberg REST API](https://github.com/apache/iceberg/blob/b59c059430d334d6a381c484fb73bb5094d00741/open-api/rest-catalog-open-api.yaml)
- [Inspected Iceberg Spark function resolution](https://github.com/apache/iceberg/blob/b59c059430d334d6a381c484fb73bb5094d00741/spark/v4.0/spark/src/main/java/org/apache/iceberg/spark/SupportsFunctions.java)
- [Inspected Iceberg catalog function/procedure integration](https://github.com/apache/iceberg/blob/b59c059430d334d6a381c484fb73bb5094d00741/spark/v4.0/spark/src/main/java/org/apache/iceberg/spark/BaseCatalog.java)
- [Databricks reference implementation revision](https://github.com/databricks/dbt-databricks/tree/2e81485830fc07bf36952069993c09da67506004)
