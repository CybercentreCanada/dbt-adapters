---
title: dbt-spark set_partition_overwrite_mode Behavior Flag
version: "1.0"
date_created: 2026-11-04
last_updated: 2026-11-04
tags: [design, dbt-spark, behavior-flags, incremental, partitioning]
---

# Introduction

This specification defines the existing `set_partition_overwrite_mode` flag,
which controls emission of a Spark session setting before eligible incremental
materializations. It does not redefine the underlying table provider's overwrite
semantics.

## 1. Purpose & Scope

The audience is adapter maintainers, implementation agents, and test authors.
Scope is the session `SET` gate in the SQL/Python incremental materialization,
including dbt-core microbatch execution. Streaming models, table replacements
outside incremental materialization, and general session configuration are
outside the flag's scope.

## 2. Definitions

| Term | Definition |
| --- | --- |
| Behavior flag | Project-level boolean under `flags` in `dbt_project.yml`. |
| Dynamic partition overwrite | Replacement of partitions represented by the write, subject to the provider's semantics. |
| Microbatch | dbt-core incremental strategy splitting bounded processing into batches. |
| Iceberg | Apache Iceberg, which handles its native overwrite behavior independently of this defensive setting. |
| Pre-hook | User-configured statement executed before the materialization's target-data operations. |

## 3. Requirements, Constraints & Guidelines

- **REQ-001**: Register `set_partition_overwrite_mode` with default `true`.
- **REQ-002**: Emit `SET spark.sql.sources.partitionOverwriteMode = DYNAMIC`
  only when the validated incremental strategy is `insert_overwrite` or
  `microbatch`, `partition_by` is truthy, and the flag is true.
- **REQ-003**: Execute the `SET` as its own statement before pre-hooks and
  before the new-target, replacement/full-refresh, or existing-target branch.
  The gate is not restricted to a write that eventually uses `INSERT OVERWRITE`.
- **REQ-004**: When disabled or other gate conditions are false, emit no `SET`
  from this gate and leave the session's current overwrite mode unchanged.
- **REQ-005**: Apply the same gate to SQL and Python incremental models.
- **REQ-006**: Keep strategy validation and provider-specific overwrite SQL
  unchanged. Disabling the flag shall not enable an unsupported strategy/format.
- **CON-001**: The flag controls statement emission, not guaranteed final
  session state; later pre-hooks or other session users may change the setting.
- **CON-002**: The materialization does not restore the prior mode after execution.
- **CON-003**: The setting is defensive for Iceberg; it is not the mechanism
  that implements Iceberg's native dynamic overwrite.
- **CON-004**: No once-per-invocation gate is applied to this `SET`; each eligible
  materialization/batch may execute it independently of metadata synchronization.
- **GUD-001**: Validate statement emission separately from provider data effects.
  Do not claim the flag alone guarantees dynamic behavior for every format.

## 4. Interfaces & Data Contracts

```yaml
flags:
  set_partition_overwrite_mode: false
```

| Strategy | Truthy `partition_by` | Flag | Emit setting |
| --- | --- | --- | --- |
| `insert_overwrite` or `microbatch` | Yes | true | Yes. |
| `insert_overwrite` or `microbatch` | Yes | false | No. |
| `insert_overwrite` or `microbatch` | No | Either | No. |
| `append` or `merge` | Either | Either | No. |

The table describes this gate; separate validation can reject a model before
reaching it. In particular, microbatch requires Iceberg and partition configuration.

```sql
set spark.sql.sources.partitionOverwriteMode = DYNAMIC
```

The existing overwrite generator uses `INSERT OVERWRITE <target>` without a
legacy partition clause for an Iceberg relation; non-Iceberg targets use
`INSERT OVERWRITE TABLE <target>` and the configured partition clause.

## 5. Acceptance Criteria

- **AC-001 (REQ-001, REQ-002)**: Given each truth-table combination, when the
  incremental materialization reaches the gate, then it emits exactly the
  specified setting or no setting.
- **AC-002 (REQ-003)**: Given an eligible model, when statements are recorded,
  then the `SET` precedes pre-hooks and branch-specific target creation/write
  even for a missing target or full refresh.
- **AC-003 (REQ-004)**: Given the flag is false and an existing session value,
  when the gate is rendered/executed, then it does not reset or restore that value.
- **AC-004 (REQ-005)**: Given equivalent SQL and Python incremental models, when
  their gate inputs match, then their emission decisions match.
- **AC-005 (REQ-006)**: Given microbatch with a non-Iceberg format, when the flag
  is false, then the existing unsupported-strategy validation still fails.
- **AC-006 (CON-001, CON-004)**: Given multiple eligible batches, when they run,
  then the setting can be emitted per batch; it is not suppressed by the
  metadata synchronization guard or promised to survive later hooks.

## 6. Test Automation Strategy

- **Test levels and frameworks**: Existing pytest/unittest Jinja harness for
  the full gate matrix and statement ordering; existing functional incremental
  tests for provider-specific partition replacement.
- **Test data management**: Render with mocked statements; live tests use two
  isolated partitions to verify that an untouched partition is retained. Save
  and restore session settings in fixtures, not through production behavior.
- **CI/CD integration**: Unit matrix belongs in the existing unit runner.
  Live validation requires the provider's configured functional profile.
- **Coverage requirements**: Both flag values, supported strategies, null/empty/
  configured partitions, both model languages, new/existing/full-refresh
  branches, and repeated batches. No numeric threshold is specified.
- **Performance testing**: An eligible gate emits one setting per execution;
  ineligible gates emit zero. No throughput guarantee is imposed.

From `dbt-spark`, run related existing unit coverage with:

```bash
hatch run unit-tests -- tests/unit/test_microbatch_macros.py tests/unit/test_macros.py
```

The existing tests cover strategy validation and overwrite SQL; a full flag-gate
matrix remains required coverage for future changes, not a claim about current
test completeness.

## 7. Rationale & Context

Non-Iceberg data-source writes can depend on session overwrite mode. Explicit
emission makes the intended default visible, while the opt-out supports engines
where changing a session setting is unsuitable. Iceberg's native overwrite
syntax remains separate from this compatibility measure.

## 8. Dependencies & External Integrations

### External Systems
- **EXT-001**: Spark execution engine and configured target table provider.
### Third-Party Services
- **SVC-001**: None independently required by this flag.
### Infrastructure Dependencies
- **INF-001**: A connection able to execute the session setting when enabled.
### Data Dependencies
- **DAT-001**: Valid strategy, partition configuration, and target provider metadata.
### Technology Platform Dependencies
- **PLT-001**: Existing dbt incremental materialization, hooks, and statement execution.
### Compliance Dependencies
- **COM-001**: Deployment session-configuration policies apply; opt-out does not
  bypass provider authorization or strategy validation.

## 9. Examples & Edge Cases

```yaml
flags:
  set_partition_overwrite_mode: true
models:
  example:
    +materialized: incremental
    +incremental_strategy: insert_overwrite
    +partition_by: [event_date]
    +location_root: /warehouse/example
```

This emits the setting before pre-hooks even on initial table creation. An
`append` model with the same partitions emits no setting. Disabling the flag
does not emit `STATIC`; it emits nothing. Bounded microbatch and Structured
Streaming are distinct and only the former uses this gate.

## 10. Validation Criteria

Compare the flag default and exact conjunction/order to the incremental macro.
Verify that opt-out does not alter overwrite SQL, strategy validation, or
metadata synchronization. Tests must distinguish session-setting control from
actual provider semantics and must not infer data correctness from emitted SQL
alone.

## 11. Related Specifications / Further Reading

- [Flag registration](../dbt-spark/src/dbt/adapters/spark/impl.py)
- [Incremental gate](../dbt-spark/src/dbt/include/spark/macros/materializations/incremental/incremental.sql)
- [Overwrite strategies](../dbt-spark/src/dbt/include/spark/macros/materializations/incremental/strategies.sql)
- [Microbatch macro tests](../dbt-spark/tests/unit/test_microbatch_macros.py)
- [Functional incremental strategies](../dbt-spark/tests/functional/adapter/incremental_strategies/test_incremental_strategies.py)
- [Streaming specification](../dbt-spark/specs/spec-design-spark-streaming-models.md)
