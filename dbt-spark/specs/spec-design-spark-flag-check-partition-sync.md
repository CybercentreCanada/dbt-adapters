---
title: dbt-spark check_partition_sync Behavior Flag
version: "1.0"
date_created: 2026-11-04
last_updated: 2026-11-04
tags: [design, dbt-spark, behavior-flags, iceberg, partitioning]
---

# Introduction

This specification defines the existing `check_partition_sync` behavior flag,
which detects differences between configured partitions and an existing Apache
Iceberg table's partition specification. It documents current implementation
boundaries; it does not authorize changes to partitioning behavior.

## 1. Purpose & Scope

The audience is adapter maintainers, implementation agents, and test authors.
Scope includes the flag registration, the macro-accessible
`adapter.check_partition_sync()` method, partition normalization, and the
incremental and streaming callers. Partition evolution and automatic repair
are out of scope.

## 2. Definitions

| Term | Definition |
| --- | --- |
| Behavior flag | A project-level boolean under `flags` in `dbt_project.yml`. |
| Iceberg | Apache Iceberg, a table format supported by Spark. |
| Partition specification | Ordered partition columns or transforms defining a table's partitioning. |
| Partition drift | A difference between normalized desired and existing partition specifications. |
| DDL | Data definition language; here, the `CREATE TABLE` statement returned by Spark. |
| Microbatch | dbt-core incremental execution split into bounded batches, distinct from Structured Streaming. |

## 3. Requirements, Constraints & Guidelines

- **REQ-001**: Register `check_partition_sync` with default `false`.
- **REQ-002**: The check shall return without reading metadata or normalizing
  configuration when the flag is false or the method's `file_format` argument
  is not exactly `"iceberg"`.
- **REQ-003**: When enabled for Iceberg, retrieve the existing specification
  through `fetch_partition_spec`, which executes `SHOW CREATE TABLE`.
- **REQ-004**: Normalize configured and existing partition expressions with the
  same grammar and compare their ordered lists. Insignificant spacing and
  equivalent quoting shall compare equal; column order, transform arguments,
  quoted identifier boundaries, and real differences shall remain significant.
- **REQ-005**: For session connections, use the live Spark
  `spark.sql.caseSensitive` setting. For other connections, normalization shall
  be case-insensitive.
- **REQ-006**: A valid unpartitioned table and `partition_by: null` or `[]` shall
  normalize to empty specifications. Invalid configuration, malformed DDL, or
  unexpected metadata result shape shall raise an explicit runtime error,
  including when drift is configured to warn only.
- **REQ-007**: On drift, warn when `check_partition_sync_raises` is false and
  raise `DbtRuntimeError` when it is true. Include the relation, desired
  partitions, and existing partitions in the diagnostic.
- **CON-001**: Detection shall not alter the table's partition specification.
- **CON-002**: Incremental callers run the check in the existing-table update
  branch, subject to the once-per-target/invocation metadata guard for
  microbatches. They do not check newly created or replaced targets.
- **CON-003**: Streaming callers check an existing target before starting or
  reusing a query; they do not stop an active query on drift.
- **CON-004**: The method gates on its supplied format, not a catalog provider
  lookup. Incremental callers currently pass `config.get('file_format')`;
  a missing/null raw value bypasses the check even though table creation
  defaults to Iceberg. Do not claim default-format inference in this check.
- **GUD-001**: Preserve the shared normalization rules in the existing adapter
  correctness specification rather than introducing a second parser.

## 4. Interfaces & Data Contracts

```yaml
flags:
  check_partition_sync: true
  check_partition_sync_raises: false
```

```text
adapter.check_partition_sync(relation, file_format, partition_by) -> None
partition_by: null | string | list[string]
SHOW CREATE TABLE result: exactly one row containing exactly one non-empty DDL string
```

Supported normalized expressions include column references, `years`, `months`,
`days`, `hours`, `bucket`, and `truncate`; bucket/truncate widths must be positive
integers. One configuration list entry represents one expression.

| Detection flag | Supplied format | Behavior |
| --- | --- | --- |
| false | Any | No check. |
| true | Not `"iceberg"`, including null | No check. |
| true | `"iceberg"` | Fetch, normalize, compare, then warn or raise on drift. |

The `@available.parse` fallback is a no-op during parse-only evaluation.

## 5. Acceptance Criteria

- **AC-001 (REQ-001, REQ-002)**: Given an omitted/false flag or non-Iceberg
  format, when the method is called even with invalid partition input, then
  it does not fetch metadata or attempt comparison.
- **AC-002 (REQ-003, REQ-004)**: Given equivalent transforms and quoting, when
  checked against valid table DDL, then no drift diagnostic is produced.
- **AC-003 (REQ-004, REQ-007)**: Given a changed order or transform width, when
  checked, then the configured warning/error contains both specifications.
- **AC-004 (REQ-005)**: Given configured `ID` and existing `id`, when session
  case sensitivity changes from false to true, then comparison changes from
  equal to drift.
- **AC-005 (REQ-006)**: Given malformed metadata or invalid configuration, when
  warning mode is enabled, then execution still fails explicitly.
- **AC-006 (CON-001 through CON-004)**: Given existing-target incremental or
  streaming execution, when callers reach the check, then its actual format
  and metadata guards apply without automatically altering partitions.

## 6. Test Automation Strategy

- **Test levels and frameworks**: pytest unit tests for the method, normalization,
  flags, and macro guards; existing local Spark/Iceberg functional tests for live
  partition comparisons and warning/error behavior.
- **Test data management**: Use mock DDL for unit tests and isolated test schemas
  for functional tests; retain the existing test fixture cleanup.
- **CI/CD integration**: Keep unit coverage in the adapter's existing unit runner.
  Run local functional tests only with the configured session profile.
- **Coverage requirements**: Both flag values, explicit/non-Iceberg/null formats,
  equal/drifted/unpartitioned specifications, case sensitivity, malformed input,
  incremental guards, and streaming call sites. No numeric threshold is imposed.
- **Performance testing**: Disabled/non-Iceberg checks must make zero metadata
  calls; one eligible method invocation fetches table DDL once.

From `dbt-spark`, focused unit validation is:

```bash
hatch run unit-tests -- tests/unit/test_partition_sync.py tests/unit/test_session_correctness.py tests/unit/test_microbatch_macros.py
```

## 7. Rationale & Context

Creating a partitioned table does not reconcile a pre-existing table's layout.
Opt-in detection exposes mismatches without silently changing storage semantics.
The separate raises flag permits advisory adoption before enforcing failure.
Existing specs discuss normalization fixes, but do not define this flag's
default, gates, callers, and diagnostics as a complete contract.

## 8. Dependencies & External Integrations

### External Systems
- **EXT-001**: Spark catalog supporting `SHOW CREATE TABLE` for Iceberg targets.
### Third-Party Services
- **SVC-001**: None required independently of the configured Spark deployment.
### Infrastructure Dependencies
- **INF-001**: Metadata read access to the target; a live Spark session for
  session-specific case sensitivity.
### Data Dependencies
- **DAT-001**: Valid table DDL and configured partition expressions.
### Technology Platform Dependencies
- **PLT-001**: Existing dbt adapter behavior registration and SQL parsing support.
### Compliance Dependencies
- **COM-001**: No additional framework is mandated; deployment access policies apply.

## 9. Examples & Edge Cases

```yaml
flags:
  check_partition_sync: true
models:
  example:
    +file_format: iceberg
    +partition_by: ["days(event_time)", "region"]
```

An existing `PARTITIONED BY (region)` emits a warning by default. Equivalent
`days( event_time )` spelling does not cause drift. Setting only the raises flag
does not enable checking. This check is not restricted to the `merge` strategy:
it is also called on other existing-target incremental update paths.

## 10. Validation Criteria

All acceptance criteria must be represented by automated coverage when changing
the behavior. Verify both registered defaults and call-site gates against source,
and distinguish proposed tests from executed validation. No partition evolution
or inferred-format behavior may be claimed by this specification.

## 11. Related Specifications / Further Reading

- [Error-on-drift flag](spec-design-spark-flag-check-partition-sync-raises.md)
- [Adapter implementation](../dbt-spark/src/dbt/adapters/spark/impl.py)
- [Partition processor](../dbt-spark/src/dbt/adapters/spark/relation_configs/partitions.py)
- [Incremental materialization](../dbt-spark/src/dbt/include/spark/macros/materializations/incremental/incremental.sql)
- [Streaming materialization](../dbt-spark/src/dbt/include/spark/macros/materializations/streaming.sql)
- [Partition unit tests](../dbt-spark/tests/unit/test_partition_sync.py)
- [Live metadata tests](../dbt-spark/tests/functional/adapter/test_metadata_session.py)
- [Adapter correctness specification](../dbt-spark/specs/spec-design-spark-adapter-correctness.md)
