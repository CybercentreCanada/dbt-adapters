---
title: dbt-spark Python Incremental Staging Locations
version: "1.0"
date_created: 2026-11-03
last_updated: 2026-11-04
tags: [design, dbt-spark, spark, python, incremental, iceberg]
---

# Introduction

This specification defines how dbt-spark assigns storage locations to physical
staging tables created during Python incremental model runs. The staging table
must use a different location from the permanent model target to prevent storage
path collisions.

## 1. Purpose & Scope

The purpose is to define location behavior for Python incremental materialization
when an existing target requires a physical staging table. The intended audience
is dbt-spark maintainers, implementation agents, and test authors.

The specification applies when all of the following are true:

- The model is a Python incremental model.
- The target already exists and the incremental materialization stages model output.
- The submission method requires a physical staging table rather than a temporary
  view.
- The model uses `location_root` to determine its storage location.

The specification covers ordinary incremental runs and microbatch runs. It does
not change SQL model behavior, permanent Python model locations, full-refresh
behavior, new-target writes, or Python submission methods that stage through
temporary views.

## 2. Definitions

| Term | Definition |
| --- | --- |
| dbt-spark | The dbt adapter for Apache Spark and Databricks. |
| Python incremental model | A Python model materialized incrementally by dbt-spark. |
| Target relation | The permanent database relation represented by the model. |
| Staging relation | An internal relation used to hold newly computed model data before the incremental strategy applies it to the target. |
| Physical staging table | A persisted table used as the staging relation, as opposed to a session-scoped temporary view. |
| Temporary view | A non-persistent Spark view used for staging within a session. |
| `location_root` | Model configuration that supplies the parent path used to derive a model's storage location. |
| Relation identifier | The identifier component of a relation name, excluding catalog, database, and schema components. |
| Microbatch | An incremental execution split into batches; the staging relation may include a batch-specific suffix. |
| Submission method | The configured mechanism used to execute a Python model. Some methods require persisted tables because work executes outside the Spark session that created a temporary view. |

## 3. Requirements, Constraints & Guidelines

### Requirements

- **REQ-001**: When an existing-target Python incremental run creates a physical
  staging table and `location_root` is configured, the staging table's location
  shall be derived from the final staging relation identifier.
- **REQ-002**: The staging table location shall differ from the permanent target's
  location. Under the default incremental naming convention, the staging
  identifier includes `__dbt_tmp`; a microbatch identifier may also include a
  batch-specific suffix.
- **REQ-003**: The permanent Python model location shall continue to be derived
  from the model alias and configured `location_root`.
- **REQ-004**: A successful incremental operation shall clean up its physical
  staging table using the existing cleanup behavior.
- **REQ-005**: The location behavior shall apply to the shared physical Python
  staging path, independent of which supported incremental strategy consumes the
  staged data.
- **REQ-006**: When `location_root` is not configured, existing location behavior
  and `require_location_root` validation shall remain unchanged.
- **REQ-007**: If the incremental staging-relation hook changes the final staging
  identifier, location derivation shall use the resulting staging relation,
  while preserving the hook's relation-routing behavior.

### Constraints

- **CON-001**: Do not change SQL model locations or SQL temporary-view staging.
- **CON-002**: Do not change permanent Python model paths, including Python
  write-order and streaming paths that target permanent relations.
- **CON-003**: Do not change new-target or full-refresh lifecycle behavior; those
  paths do not use the existing-target physical staging table covered here.
- **CON-004**: Python submission methods that stage through temporary views shall
  retain their current view and location behavior.
- **CON-005**: Keep the fix within dbt-spark unless a separately approved
  cross-package change is required.

### Guidelines

- **GUD-001**: Use the final staging relation as the source of truth for the
  staging location rather than rebuilding the staging identifier independently.
- **GUD-002**: Keep permanent and staging location derivation explicit enough to
  prevent a future caller change from silently moving permanent model data.
- **GUD-003**: A functional test must use a submission method and profile that
  actually create physical Python staging tables.

## 4. Interfaces & Data Contracts

### Location derivation

When `location_root` is configured, the location contract is:

```text
permanent_location = location_root + "/" + model_alias
staging_location   = location_root + "/" + final_staging_relation_identifier
```

For example:

```text
location_root = "dbfs:/warehouse/models"
model_alias = "orders"
staging_relation_identifier = "orders__dbt_tmp"

permanent_location = "dbfs:/warehouse/models/orders"
staging_location   = "dbfs:/warehouse/models/orders__dbt_tmp"
```

For microbatch staging, the final identifier can include a batch suffix:

```text
staging_relation_identifier = "orders__dbt_tmp_batch_1"
staging_location = "dbfs:/warehouse/models/orders__dbt_tmp_batch_1"
```

### Integration points

| Surface | Contract |
| --- | --- |
| Python location macro | Must retain the permanent-model default and accept or otherwise access the final target/staging relation when deriving a physical Python write location. |
| Python table writer | Must pass the relation it is writing to the location derivation path. Permanent writes must continue to resolve to the model alias location. |
| Incremental materialization | Must continue to choose temporary views versus physical staging tables according to submission method, use the resolved staging relation, apply the incremental strategy, and preserve cleanup. |
| Staging-relation hook | Must continue to control the staging relation's routing. Location derivation must follow the resolved relation identifier. |
| SQL writer | No behavior or interface change is required. |

## 5. Acceptance Criteria

- **AC-001 (REQ-001, REQ-002)**: Given an existing Python incremental target and a
  physical staging submission method, when `location_root` is configured and the
  model runs incrementally, then the physical staging write uses a location
  derived from its staging relation identifier and not the permanent target
  location.
- **AC-002 (REQ-003)**: Given a permanent Python model write, when
  `location_root` is configured, then the location remains
  `location_root/<model_alias>`.
- **AC-003 (REQ-002)**: Given a microbatch run, when a physical staging relation
  includes a batch suffix, then the staging location includes that complete
  relation identifier.
- **AC-004 (REQ-004)**: Given a successful physical-staging incremental run, when
  the strategy completes, then the staging relation is removed using the
  established cleanup behavior.
- **AC-005 (CON-001, CON-004)**: Given a SQL incremental model or a Python model
  using temporary-view staging, when it runs, then it retains its previous
  location and staging behavior.
- **AC-006 (CON-003)**: Given a new target or a full-refresh run, when it writes
  the model, then it does not receive a location change from this specification.
- **AC-007 (REQ-006)**: Given a model without `location_root`, when it writes
  either its permanent target or staging relation, then existing required-root
  validation and location behavior remain in effect.
- **AC-008 (REQ-007)**: Given a custom staging-relation hook that changes the
  staging identifier, when the model creates a physical staging table, then the
  location follows the resolved relation identifier without bypassing the hook.
- **AC-009**: Given an existing target whose permanent and staging relations
  would otherwise resolve to the same storage path, when the incremental run is
  executed, then staging-table creation succeeds without a path-collision error.

## 6. Test Automation Strategy

- **Test levels**:
  - **Unit**: Render the Python location macro with a permanent relation, a
    default `__dbt_tmp` relation, a microbatch-suffixed relation, and a relation
    represented by a fully qualified string if that input form remains supported.
    Assert permanent paths remain alias-based and physical staging paths use the
    staging identifier.
  - **Functional**: Run a Python Iceberg incremental model with a physical-table
    submission method at least twice against an existing target. Assert the
    second run succeeds, the permanent target remains available at its expected
    path, and the staging relation is cleaned up afterward. Test through a
    supported Spark/Databricks profile.
- **Frameworks**: Use the dbt-spark unit and functional test suites and existing
  dbt test fixtures. Do not introduce a separate test framework.
- **Local integration coverage**: The `local_spark_iceberg` profile runs default
  physical staging and temporary-view compatibility through `dbt run`, using a
  test-only local submission helper for the physical case. It checks actual
  catalog locations, merged rows, and physical staging cleanup. It does not
  certify remote Databricks submission, custom staging hooks, or physical
  microbatch execution.
- **Test data management**: Use a unique temporary storage root for functional
  tests. Tests must not use production storage locations or credentials.
- **CI/CD integration**: Run unit tests in the existing dbt-spark unit-test job.
  Run the functional test only in an integration environment configured for the
  required Python submission method and Iceberg catalog.
- **Coverage requirements**: Cover default staging identifiers, microbatch
  suffixes, permanent-path compatibility, view-path compatibility, and cleanup.
  No numeric coverage threshold is specified.
- **Performance testing**: No separate performance benchmark is required; the
  location selection must not introduce additional Spark queries or writes.

## 7. Rationale & Context

The incremental materialization distinguishes the staging relation name from the
permanent target. With `location_root`, deriving both physical table locations
from the model alias can cause the staging and target tables to claim the same
storage path. A catalog may reject staging-table creation or the two relations
may interfere with each other.

Using the resolved staging relation identifier gives the physical staging table
its own path and naturally includes the standard temporary suffix and
microbatch-specific suffix. Restricting the behavior to physical Python staging
preserves the established paths for permanent models and avoids changing SQL or
session-scoped temporary views.

The fix is general adapter behavior. Deployment may begin in development
environments before broader rollout; the adapter code itself is not environment
gated.

Live local integration testing also exposed a prerequisite: Iceberg ignores
DataFrameWriterV2's `option("location", ...)`. Python Iceberg writers must use
`tableProperty("location", ...)` to honor this specification's paths. The
alias-based permanent path contract is unchanged, but newly created Iceberg
Python tables now actually use the configured path rather than silently using
the catalog default. This also applies to the shared writer used for streaming
table initialization. Existing tables are not relocated, and non-Iceberg
location options are unchanged.

## 8. Dependencies & External Integrations

### External Systems

- **EXT-001**: Apache Spark or Databricks - Executes Python incremental model
  work and creates physical staging tables where required by the submission
  method.

### Third-Party Services

- **SVC-001**: None required for the adapter behavior itself.

### Infrastructure Dependencies

- **INF-001**: A storage system supported by the configured Spark catalog and
  accessible to both permanent and staging relations.
- **INF-002**: Functional integration infrastructure with a submission method
  capable of creating physical Python staging tables.

### Data Dependencies

- **DAT-001**: A Python incremental model and an existing target relation are
  required to exercise the staging path. Functional tests must use isolated,
  disposable test data.

### Technology Platform Dependencies

- **PLT-001**: A dbt-spark-compatible Spark or Databricks runtime with Python
  model support and the configured Iceberg/catalog capabilities for the
  functional test.

### Compliance Dependencies

- **COM-001**: No additional regulatory requirements are introduced. Test
  credentials and production storage locations must not be committed or used by
  automated tests.

## 9. Examples & Edge Cases

### Existing Python incremental target using physical staging

```text
Model alias: orders
location_root: dbfs:/warehouse/models
Target identifier: orders
Staging identifier: orders__dbt_tmp

Target location:  dbfs:/warehouse/models/orders
Staging location: dbfs:/warehouse/models/orders__dbt_tmp
```

### Microbatch staging

```text
Model alias: orders
location_root: dbfs:/warehouse/models
Staging identifier: orders__dbt_tmp_batch_1

Staging location: dbfs:/warehouse/models/orders__dbt_tmp_batch_1
```

### Compatibility cases

| Case | Expected behavior |
| --- | --- |
| Permanent Python model write | Keep `location_root/<model_alias>`. |
| Existing-target SQL incremental run | Preserve SQL temporary-view behavior. |
| Python `spark_session_based_cluster` staging | Preserve temporary-view behavior. |
| Python physical staging without `location_root` | Preserve current root validation and location behavior. |
| New target or full refresh | Preserve existing write path; do not apply staging-specific naming. |
| Custom staging relation | Use the resolved relation's identifier and retain relation routing. |

## 10. Validation Criteria

A change conforms to this specification only when all of the following hold:

1. Unit tests demonstrate distinct permanent and physical staging paths for
   default and microbatch staging identifiers.
2. Unit tests demonstrate that permanent Python locations remain alias-based.
3. SQL and temporary-view staging paths remain unchanged.
4. The Python incremental materialization still cleans up a physical staging
   table after the strategy completes.
5. A functional existing-target Python incremental rerun succeeds with
   `location_root` and an Iceberg-capable physical Python submission method.
6. Functional test cleanup leaves the permanent target available and removes
   the staging relation.
7. Python syntax checks and the repository's focused dbt-spark unit tests pass.
8. The implementation is not gated by deployment environment, and no credentials
   or production storage locations are added to the repository.

## 11. Related Specifications / Further Reading

- [dbt-spark Iceberg write-order specification](./spec-design-iceberg-write-order.md)
- [Spark incremental materialization](../src/dbt/include/spark/macros/materializations/incremental/incremental.sql)
- [Spark Python table writer and location macros](../src/dbt/include/spark/macros/materializations/table.sql)
- [Spark adapter location macros](../src/dbt/include/spark/macros/adapters.sql)
- [Spark Python model functional tests](../tests/functional/adapter/test_python_model.py)
- [Local Python staging integration tests](../tests/functional/adapter/test_python_incremental_staging_location.py)
