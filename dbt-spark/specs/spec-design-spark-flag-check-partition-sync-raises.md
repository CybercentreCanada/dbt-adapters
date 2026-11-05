---
title: dbt-spark check_partition_sync_raises Behavior Flag
version: "1.0"
date_created: 2026-11-04
last_updated: 2026-11-04
tags: [design, dbt-spark, behavior-flags, iceberg, partitioning]
---

# Introduction

This specification defines the existing `check_partition_sync_raises` flag.
It selects error rather than warning behavior for detected Iceberg partition
drift; it does not independently enable drift detection.

## 1. Purpose & Scope

The audience is adapter maintainers, implementation agents, and test authors.
Scope is the severity decision in `adapter.check_partition_sync()` and its
effect on incremental and streaming callers. Partition parsing and detection
are defined in the companion detection-flag specification. Automatic partition
repair, transaction rollback, and stopping running streams are excluded.

## 2. Definitions

| Term | Definition |
| --- | --- |
| Behavior flag | A project-level boolean under `flags` in `dbt_project.yml`. |
| Partition drift | Unequal normalized configured and existing partition specifications. |
| Iceberg | Apache Iceberg, the table format to which the partition check applies. |
| Warning mode | Log detected drift and return without raising for that drift. |
| Error mode | Raise `DbtRuntimeError` for detected drift. |

## 3. Requirements, Constraints & Guidelines

- **REQ-001**: Register `check_partition_sync_raises` with default `false`.
- **REQ-002**: The flag shall affect severity only when `check_partition_sync`
  is true, the supplied `file_format` is exactly `"iceberg"`, and valid desired
  and existing specifications differ.
- **REQ-003**: When true, detected drift shall raise `DbtRuntimeError` instead
  of logging the drift warning. The diagnostic shall identify the target and
  show configured and existing normalized partitions.
- **REQ-004**: When false, detected drift shall be logged with `logger.warning`;
  the check shall then return normally without changing table partitions.
- **REQ-005**: Equal specifications shall produce neither a drift warning nor
  a drift exception, regardless of this flag.
- **REQ-006**: Metadata retrieval failures, malformed metadata, and invalid
  partition configuration shall remain explicit errors even when this flag is
  false. Warning mode shall not suppress operational or validation errors.
- **CON-001**: Setting this flag to true alone shall not trigger metadata reads
  or drift detection.
- **CON-002**: An error shall propagate from the check and prevent the caller's
  subsequent steps from executing in that call. It does not roll back earlier
  hooks or metadata work and does not guarantee atomic model execution.
- **CON-003**: For streaming models, failure shall not stop a query already
  running in Spark. Existing-query checks occur before startup/reuse handling.
- **GUD-001**: Test severity independently from the shared partition parser.

## 4. Interfaces & Data Contracts

```yaml
flags:
  check_partition_sync: true
  check_partition_sync_raises: true
```

| Detection enabled | Raises enabled | Format/input condition | Result |
| --- | --- | --- | --- |
| false | Either | Any | No check. |
| true | Either | Supplied format is not `"iceberg"` | No check. |
| true | Either | Valid equal specifications | Normal return. |
| true | false | Valid drift | Warning, normal return. |
| true | true | Valid drift | `DbtRuntimeError`. |
| true | Either | Eligible check cannot read/parse/validate its inputs | Explicit error. |

The drift diagnostic begins with `Partition drift detected on <relation>.`
and includes `Model partition_by config:` and `Existing table partitions:`.

## 5. Acceptance Criteria

- **AC-001 (REQ-001, REQ-002, CON-001)**: Given only this flag is true, when the
  method is invoked, then no partition metadata is fetched.
- **AC-002 (REQ-003)**: Given both flags are true and valid Iceberg drift exists,
  when checked, then a runtime error contains the relation and both specifications.
- **AC-003 (REQ-004)**: Given detection is true and raises is false, when drift
  is checked, then a warning is recorded, the method returns, and the check does
  not alter partitions.
- **AC-004 (REQ-005)**: Given equal specifications, when either severity mode is
  used, then there is no drift warning or exception.
- **AC-005 (REQ-006)**: Given invalid metadata or partition configuration, when
  raises is false, then the invalid check still fails.
- **AC-006 (CON-002)**: Given an existing incremental target and error-mode
  drift, when execution reaches the check before the incremental write, then
  the subsequent write does not run; prior work is not claimed to be rolled back.
- **AC-007 (CON-003)**: Given an active streaming query, when the existing-target
  check raises, then the dbt operation fails without stopping that Spark query.

## 6. Test Automation Strategy

- **Test levels and frameworks**: pytest unit tests with mocked metadata and
  warning capture; existing Spark/Iceberg functional tests for warn/error effects.
- **Test data management**: Use isolated DDL fixtures and temporary test schemas;
  verify the table's original partition spec and row counts after drift checks.
- **CI/CD integration**: Existing adapter unit workflow; local functional
  execution with the configured session profile.
- **Coverage requirements**: Every row in the contract table, warning contents,
  exception contents, unchanged partitions, and caller propagation. No numeric
  coverage threshold is introduced.
- **Performance testing**: Severity selection must not add metadata requests
  beyond the detection method's existing single DDL fetch.

From `dbt-spark`:

```bash
hatch run unit-tests -- tests/unit/test_partition_sync.py tests/unit/test_session_correctness.py
```

The live warning/error cases reside in
`tests/functional/adapter/test_metadata_session.py`.

## 7. Rationale & Context

Separating detection from severity supports advisory rollout without making
warning mode a general error-suppression mechanism. A drift warning concerns
two valid but different specifications; unreadable or malformed inputs cannot
establish that comparison and must fail explicitly.

## 8. Dependencies & External Integrations

### External Systems
- **EXT-001**: Spark/Iceberg metadata facilities used by the detection method.
### Third-Party Services
- **SVC-001**: None beyond the configured Spark deployment.
### Infrastructure Dependencies
- **INF-001**: Target metadata read access when detection is eligible.
### Data Dependencies
- **DAT-001**: Normalized desired and existing partition specifications.
### Technology Platform Dependencies
- **PLT-001**: Existing dbt behavior flags, adapter logging, and runtime error types.
### Compliance Dependencies
- **COM-001**: No additional compliance framework is imposed.

## 9. Examples & Edge Cases

```yaml
# Advisory drift detection.
flags:
  check_partition_sync: true
  check_partition_sync_raises: false
```

With desired `["region"]` and existing `["days(event_time)"]`, advisory mode warns.
Changing raises to true makes the same drift an error. Reordering two otherwise
equal partition expressions also constitutes drift. A malformed `partition_by`
value fails in both modes.

## 10. Validation Criteria

The truth table, default, error class, diagnostics, and unchanged partition
layout must match source and test evidence. Do not equate warning mode with
ignoring validation failures or error mode with automatic transaction rollback.
This documentation-only specification does not claim that its full acceptance
matrix has been executed.

## 11. Related Specifications / Further Reading

- [Detection-flag contract](spec-design-spark-flag-check-partition-sync.md)
- [Adapter implementation](../dbt-spark/src/dbt/adapters/spark/impl.py)
- [Session correctness tests](../dbt-spark/tests/unit/test_session_correctness.py)
- [Live warning/error tests](../dbt-spark/tests/functional/adapter/test_metadata_session.py)
- [Streaming model specification](../dbt-spark/specs/spec-design-spark-streaming-models.md)
