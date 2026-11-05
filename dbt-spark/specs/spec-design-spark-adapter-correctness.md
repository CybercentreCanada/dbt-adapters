---
title: dbt-spark Adapter Correctness and Reliability
version: "1.0"
date_created: 2026-11-03
last_updated: 2026-11-03
tags: [design, dbt-spark, correctness, reliability, testing]
---

# Introduction

This specification defines proposed remediation requirements for eight Spark
adapter correctness findings. Findings were reproduced using isolated mocks or
import isolation; they are not evidence of production incidents or certification
of a live Spark runtime. Creating or approving this document does not authorize
implementation of adapter fixes.

## 1. Purpose & Scope

The intended audience is dbt-spark maintainers, implementation agents, and test
authors. Scope is the Python package under `dbt-spark/src/dbt/adapters/spark`,
directly related tests, documentation, and changelog entries for future fixes.

Required work covers query replay, optional dependency loading, catalog column
identity, relation listing, session cursor result lifecycle and context handling,
Databricks command error reporting, and partition comparison.

Unrelated refactors, new connection methods, global runtime upgrades, CI changes,
and optional improvements in Section 7 are excluded. Existing worktree changes
must be preserved. Any intentional automatic SQL replay policy requires a
separate approval before implementation; this specification does not authorize
such replay.

## 2. Definitions

| Term | Definition |
| --- | --- |
| dbt | Data build tool that executes configured model materializations. |
| Adapter | dbt integration responsible for database operations and metadata. |
| SQL | Structured Query Language. |
| DDL / DML | Data definition / data manipulation language. |
| Non-idempotent operation | Operation whose repeated execution can change its result or introduce additional side effects. |
| Indeterminate execution | The client cannot establish whether an operation was accepted or completed by the server. |
| Polling | Repeatedly requesting the state of an already-submitted operation. |
| Operation handle | Driver/server identity for one submitted operation. |
| Relation | Catalog/database, schema, and identifier representing a table or view. |
| Catalog entry | Column metadata associated with a relation in generated dbt documentation. |
| Session cursor | Local Spark result interface exposing execution and fetch methods. |
| PySpark | Python interface to Apache Spark, optional for non-session installations. |
| Partition drift | A semantic difference between configured and existing partition expressions. |
| CI | Continuous integration. |

## 3. Requirements, Constraints & Guidelines

### Required remediation

- **REQ-001 (F1)**: Transport failure after SQL submission, or when acceptance is
  uncertain, shall not cause automatic resubmission. Resume polling only when
  the original operation can be identified and safely accessed; otherwise fail
  explicitly with indeterminate execution status. This applies to all SQL, not
  only statements classified as writes.
- **REQ-002 (F2)**: Loading the adapter for non-session connections shall not
  require PySpark. Local session/Python execution requiring PySpark shall fail
  with an actionable dependency error if it is unavailable.
- **REQ-003 (F3)**: Catalog generation shall emit exactly one entry per discovered
  relation column, preserving its catalog/database, schema, identifier, type,
  column name, and data type without introducing a duplicate null-database entry.
- **REQ-004 (F4)**: Regular listing shall render DESCRIBE targets using relation
  components and include/quote policies. An absent catalog shall be omitted,
  never rendered as the string `None`. Required identifier escaping shall
  preserve names containing punctuation or embedded backticks.
- **REQ-005 (F4)**: DESCRIBE permission, malformed-name, and runtime failures shall
  not become success-shaped empty metadata. An object confirmed to have
  disappeared during listing may be omitted without discarding other relations.
  Existing supported listing fallbacks shall remain available, but cannot mask
  a failure by fabricating a table or returning an empty schema.
- **REQ-006 (F5)**: Each cursor execute attempt shall reset previous result state,
  including when the new operation fails. Sequential fetch operations shall
  consume results in order without duplicating or dropping unfetched rows.
- **REQ-007 (F6)**: Cursor context exit shall release cursor result resources and
  propagate exceptions raised in the context body.
- **REQ-008 (F7)**: All-purpose Databricks command errors shall be read from the
  dictionary returned by the command-status API, not treated as an HTTP response
  object. Finished error results, Error, and Cancelled states shall fail
  explicitly while preserving available server diagnostics.
- **REQ-009 (F7)**: Execution-context cleanup shall still be attempted on success
  and failure. If command and cleanup both fail, report both and retain the
  original command failure as the causal error.
- **REQ-010 (F8)**: Model and database partition expressions shall use a shared
  semantic normalization contract. Equivalent quoting and insignificant
  whitespace shall compare equal; quoted punctuation, escaped backticks,
  transform arguments, expression order, and actual partition differences shall
  remain distinguishable.
- **REQ-011 (F8)**: Invalid partition configuration or unparseable database
  partition metadata shall fail explicitly, not silently become an unpartitioned
  table. A valid table with no partition clause remains unpartitioned.

### Constraints and guidelines

- **CON-001**: Preserve public adapter interfaces unless a related behavior shift
  is necessary and explicitly documented.
- **CON-002**: Do not claim exactly-once server execution. Prevent client replay;
  an operation with lost status may still execute remotely.
- **CON-003**: Do not require PySpark globally to conceal the optional-import bug.
- **CON-004**: Preserve missing-object behavior, relation routing, existing
  partition-check flags, and supported metadata fallbacks where compatible with
  explicit error reporting.
- **SEC-001**: Tests shall not expose or commit credentials, contact production
  systems, or write to production storage.
- **PAT-001**: Reuse existing relation rendering, driver operation identities,
  repository errors, logging, and test fixtures.
- **GUD-001**: Prefer focused fixes and regression tests over broad refactors.
  Shared normalization must preserve quoted identifier semantics rather than
  stripping delimiters indiscriminately.

## 4. Interfaces & Data Contracts

### Query execution outcome

| Condition | Required client behavior |
| --- | --- |
| Server reports success | Return existing success response. |
| Server reports error/cancellation | Raise existing database/runtime error with diagnostics. |
| Transport fails after submission or acceptance is unknown | Poll the same operation if supported; otherwise raise an indeterminate-execution error. Do not submit replacement SQL. |
| Failure demonstrably occurs before submission | Existing safe connection-establishment retry may remain; this does not authorize replay of an accepted operation. |

The existing `execute(sql, bindings)` and profile `query_retries` interfaces must
be considered during remediation. Any narrowed interpretation of query retries
must be documented. A retry counter alone is not proof that replay is safe.

### Catalog output

```text
ColumnEntry:
  table_database: relation.database or the established absent-catalog value
  table_schema: relation.schema
  table_name: relation.identifier
  table_type: accurately discovered table/view type
  column_name: discovered column identifier
  column_type: discovered data type
```

Within one relation, each discovered column occurs once. Fast-listed relations
with empty presentation metadata still obtain column schema when requested.

### Cursor results

| Method | Contract |
| --- | --- |
| `execute` | Invalidate old rows/schema; establish only the new result. |
| `fetchone` | Return and consume the next row; return `None` when exhausted. |
| `fetchall` | Return and consume all remaining rows; return `[]` for an exhausted result set. |
| `close` | Release the cursor's result references. |
| `__exit__` | Close and return a non-suppressing value. |

Behavior before any execution or after close must follow the established
cursor interface; this specification does not require inventing an empty
successful result for an invalid cursor state.

### Databricks command status

`DBCommand.status` returns a dictionary containing status and, when supplied,
result diagnostics. Error reporting shall use dictionary access with validated
API result shapes. Missing diagnostic fields shall produce an explicit message
identifying the terminal state and unavailable diagnostics, not success.

### Partition comparison

Inputs are configured `partition_by` values and database SHOW CREATE TABLE
output. The normalized representation shall preserve ordered expressions and
Spark identifier semantics. Commas inside quoted identifiers are not expression
separators. Normalization must not lower-case case-sensitive identifier content
as an unconditional substitute for semantic resolution.

## 5. Acceptance Criteria

- **AC-001 (REQ-001)**: Given an INSERT was accepted and its subsequent poll loses
  the connection, when retry handling runs, then SQL is submitted once and the
  client resumes the original operation or reports indeterminate execution.
- **AC-002 (REQ-002)**: Given PySpark is unavailable, when a non-session adapter
  is imported, then loading succeeds; when local Spark execution is selected,
  then a dependency error explains the required session extra.
- **AC-003 (REQ-003)**: Given one Iceberg column in a named catalog, when catalog
  output is generated, then exactly one entry contains that catalog identity.
  Repeat for non-Iceberg tables and views.
- **AC-004 (REQ-004)**: Given a relation without a catalog, when listing describes
  it, then the emitted target contains schema and identifier without `None`.
  With a catalog or unusual identifiers, then rendering preserves all components.
- **AC-005 (REQ-005)**: Given DESCRIBE fails for permission reasons, when listing
  completes or exhausts legitimate fallbacks, then it fails explicitly; given
  one object disappeared, then surviving relations retain accurate metadata.
- **AC-006 (REQ-006)**: Given query A was fetched, when query B executes, then
  fetches return only B's rows. If B fails, A's rows are not available as B's result.
- **AC-007 (REQ-006)**: Given three rows, when fetchone is called followed by
  fetchall, then the first call returns row one and the second returns rows two
  and three; subsequent fetches indicate exhaustion.
- **AC-008 (REQ-007)**: Given a context body raises an exception, when the cursor
  exits, then it closes and the caller receives that exception.
- **AC-009 (REQ-008, REQ-009)**: Given a command ends in Error, Cancelled, or a
  Finished error result, when the helper handles it, then it reports failure
  with available server diagnostics and attempts cleanup. Simultaneous cleanup
  failure does not hide the command failure.
- **AC-010 (REQ-010)**: Given equivalent quoted identifiers or transform spacing,
  when partition drift is checked, then no false drift is reported. Reordered
  expressions or changed transform arguments remain drift.
- **AC-011 (REQ-011)**: Given malformed configuration or metadata, when partition
  comparison runs, then it reports an explicit error; a valid unpartitioned
  table still compares equal to omitted partition configuration.

## 6. Test Automation Strategy

- **Test levels**: Isolated unit regressions for all eight findings; targeted
  local Spark integration for cursor, catalog/listing, and partition semantics;
  mocked Databricks API tests for command state/cleanup.
- **Frameworks**: Existing pytest, unittest mocks, and dbt adapter fixtures.
- **Test data management**: Disposable relations/warehouses and synthetic API
  dictionaries. Use subprocess/import isolation for absent optional dependencies.
- **CI integration**: Add regressions to existing relevant suites. New CI runtime
  matrices are outside scope.
- **Coverage requirements**: Every requirement has a regression assertion.
  Catalog tests assert counts and keys, not merely non-empty output. Replay tests
  assert submission count for writes, not merely eventual success of SELECT.
  No numeric coverage threshold is introduced.
- **Performance testing**: No separate benchmark is required. Avoid extra schema
  queries and unnecessary row collection; do not trade fetch correctness for
  unbounded eager collection without evaluating existing cursor constraints.

Review baseline: 88 tests passed across polling, partition synchronization, table
properties, and write-order unit suites. This baseline does not validate the
new requirements. Future implementation must run focused regressions, preserve
baseline behavior, and use existing code-quality tooling.

## 7. Rationale & Context

| Finding | Observed reproduction | Required correction |
| --- | --- | --- |
| F1 | Two submissions after an INSERT poll loses its connection | Do not replay indeterminate SQL. |
| F2 | Adapter import fails without optional PySpark | Defer session-only imports. |
| F3 | One Iceberg column yields two entries with different database IDs | Single, identity-preserving output path. |
| F4 | Catalog-less DESCRIBE target is `None.default.view_name` | Relation-aware rendering and explicit metadata failures. |
| F5 | Reused cursor returns old query rows | Reset and consume result state correctly. |
| F6 | Exception raised inside cursor context is suppressed | Non-suppressing context exit. |
| F7 | Error callback raises `'dict' object has no attribute 'json'` | Dictionary-based diagnostics and reliable cleanup. |
| F8 | Equivalent quoting/spacing compares unequal | Shared semantic partition normalization. |

Optional follow-up recommendations, not independent mandatory scope:

- Bound Databricks HTTP transport calls and use monotonic polling deadlines.
- Consolidate identifier escaping across the adapter beyond the rendering
  necessary for REQ-004.
- Audit unrelated broad metadata-error fallbacks.

Exact catalog-result assertions are mandatory where needed for REQ-003 even
though broader test strengthening is optional.

## 8. Dependencies & External Integrations

### External Systems

- **EXT-001**: Apache Spark - supplies session query results, relation metadata,
  partition DDL, and operation states for supported connection methods.
- **EXT-002**: Databricks command API - supplies command states and execution
  context lifecycle. Unit validation shall mock this API.

### Third-Party Services

- **SVC-001**: No new service is required. Existing remote integration profiles
  may be used for authorized live verification only.

### Infrastructure Dependencies

- **INF-001**: Isolated local Spark runtime and disposable catalog/warehouse for
  live regressions.
- **INF-002**: Driver support for original-operation polling determines whether
  recovery is possible; unavailable recovery must yield explicit failure.

### Data Dependencies

- **DAT-001**: Synthetic cursor rows, metadata, partition expressions, transport
  failures, and command-status dictionaries with both success and failure shapes.

### Technology Platform Dependencies

- **PLT-001**: Existing supported Python/dbt and Spark adapter interfaces.
  PySpark remains optional for non-session connections.
- **PLT-002**: Iceberg-capable runtime for live Iceberg catalog tests. No runtime
  version upgrade or new certification matrix is authorized by this specification.

### Compliance Dependencies

- **COM-001**: Existing repository credential-handling rules apply. No new
  regulatory requirement is introduced.

## 9. Examples & Edge Cases

```text
INSERT accepted -> poll transport failure
  allowed: reconnect/recover and poll the same operation, if supported
  allowed: explicit "execution outcome unknown; operation may still run" error
  forbidden: execute the INSERT again merely because query_retries > 0

Relation(database=None, schema="analytics", identifier="orders")
  describe target: analytics.orders, subject to relation quoting policies
  forbidden: None.analytics.orders

Rows [A, B, C]
  fetchone() -> A
  fetchall() -> [B, C]
  fetchone() -> None
  fetchall() -> []
```

Partition equivalents include a quoted regular identifier versus its unquoted
equivalent where Spark resolves both identically, and `bucket(16, id)` versus
`bucket(16,   id)`. A quoted identifier containing a comma must remain one
identifier. Case-sensitive names and different transform arguments must not be
collapsed into false equality.

## 10. Validation Criteria

Compliance requires all acceptance criteria to pass, plus:

1. No non-session import requires the optional PySpark package.
2. Unknown SQL outcomes do not silently report success or trigger replacement
   submissions.
3. Catalog entries are unique and preserve identity; missing-object handling
   does not conceal permission/runtime errors.
4. Cursor and Databricks helper failures remain observable after cleanup.
5. Partition checks preserve real differences and reject invalid inputs.
6. Focused tests and relevant code-quality checks pass; unavailable live
   environments are reported rather than treated as successful certification.
7. Documentation and changelog entries explain implemented behavior shifts,
   particularly query retry semantics.
8. Existing unrelated worktree changes remain untouched.

Current status: proposed requirements only. Isolated reproductions and the
88-test baseline are recorded; remediation and live runtime validation are not
completed or authorized.

## 11. Related Specifications / Further Reading

The following links assume publication at
`spec/spec-design-spark-adapter-correctness.md` in the repository:

- [Spark connections](../dbt-spark/src/dbt/adapters/spark/connections.py)
- [Spark adapter implementation](../dbt-spark/src/dbt/adapters/spark/impl.py)
- [Session cursor](../dbt-spark/src/dbt/adapters/spark/session.py)
- [Python submission helpers](../dbt-spark/src/dbt/adapters/spark/python_submissions.py)
- [Partition configuration](../dbt-spark/src/dbt/adapters/spark/relation_configs/partitions.py)
- [Iceberg write-order specification](../dbt-spark/specs/spec-design-iceberg-write-order.md)
- [Python incremental staging-location specification](../dbt-spark/specs/spec-design-spark-python-incremental-staging-location.md)
