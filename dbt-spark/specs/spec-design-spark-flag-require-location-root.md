---
title: dbt-spark require_location_root Behavior Flag
version: "1.0"
date_created: 2026-11-04
last_updated: 2026-11-04
tags: [design, dbt-spark, behavior-flags, storage, materializations]
---

# Introduction

This specification defines the existing `require_location_root` behavior flag,
which requires explicit table locations when the adapter renders its location
helpers. It complements, rather than replaces, the existing Python incremental
staging-location specification.

## 1. Purpose & Scope

The audience is adapter maintainers, implementation agents, and test authors.
Scope covers flag registration, SQL/Python location helpers, and their table,
seed, incremental, write-order, and streaming creation paths. It is not a
global validation of every parsed model or a storage authorization mechanism.

## 2. Definitions

| Term | Definition |
| --- | --- |
| Behavior flag | Project-level boolean under `flags` in `dbt_project.yml`. |
| `location_root` | Model or seed configuration supplying a storage parent path. |
| Alias | Final dbt model identifier used to derive a permanent table's location. |
| Physical staging table | Persisted intermediate table, distinct from a session temporary view. |
| Iceberg | Apache Iceberg, whose Python table writer uses a location table property. |
| WriterV2 | Spark DataFrameWriterV2, used by Python table creation paths. |

## 3. Requirements, Constraints & Guidelines

- **REQ-001**: Register `require_location_root` with default `true`.
- **REQ-002**: When a location helper is rendered and `location_root` is null
  or absent, raise a compiler error if the flag is true. The message shall name
  the model alias and direct the user to configure `location_root` or disable
  `require_location_root`.
- **REQ-003**: If the root is null/absent and the flag is false, emit no explicit
  location assignment and leave placement to the engine/provider.
- **REQ-004**: If a string root is supplied, use it regardless of the flag:
  the permanent location shall remain `<location_root>/<model_alias>`.
- **REQ-005**: SQL location output shall be
  `location '<location_root>/<model_alias>'`. Python output shall set
  `target_location` and use `writer.tableProperty("location", target_location)`
  for Iceberg, or `writer.option("location", target_location)` for other formats.
- **REQ-006**: Preserve physical Python staging locations derived from the
  staging relation identifier, including microbatch suffixes, as defined by
  the existing staging-location specification.
- **CON-001**: Enforcement occurs when the location helper is rendered; it does
  not promise a global parse-time check or that all runtime branches require
  a storage location. SQL temporary-view creation bypasses the location helper.
- **CON-002**: Both helpers test `location_root is not none`, not truthiness.
  Empty strings currently pass that presence check and produce `/<identifier>`;
  this contract does not introduce blank-value or URI validation.
- **CON-003**: The helpers concatenate a slash without path normalization and
  do not test writability, create directories, validate allowed storage prefixes,
  or migrate existing tables.
- **CON-004**: Rendering a helper inside generated Python may enforce the
  requirement even if a runtime branch would later skip table creation. Do not
  assume runtime temporary-view/query-reuse branches bypass Jinja validation.
- **GUD-001**: Treat explicit storage placement as configuration, not proof of
  access control or compliance. Use existing storage permissions.

## 4. Interfaces & Data Contracts

```yaml
flags:
  require_location_root: true
models:
  example:
    +location_root: /warehouse/example
seeds:
  example:
    +location_root: /warehouse/example-seeds
```

| Root | Flag | Helper result |
| --- | --- | --- |
| Null or absent | true | Compiler error. |
| Null or absent | false | No explicit location assignment. |
| String, including empty | Either | Derive and emit location. |

The SQL entry point is dispatched `location_clause()` /
`spark__location_clause()`. The Python entry point is
`python__location_clause(target_relation=none)`. The optional target relation
retains the existing staging-identifier convention; it does not redefine
permanent model locations.

| Caller | Location-helper surface |
| --- | --- |
| SQL permanent table and incremental creation/replacement | SQL location clause. |
| Seed table creation | SQL location clause. |
| SQL write-order target creation/replacement | SQL location clause passed to the existing write-order path. |
| Python table / physical staging / write-order writer | Python location clause. |
| Streaming sink initialization, for SQL or Python models | Python location clause in generated query-start code. |

The flag does not instruct Spark to change an already-existing table's location.

## 5. Acceptance Criteria

- **AC-001 (REQ-001, REQ-002)**: Given the default flag and no root, when either
  helper is rendered, then compilation fails with the alias and both corrective
  options in the error.
- **AC-002 (REQ-003)**: Given the flag is false and no root, when either helper
  is rendered, then no explicit location assignment is emitted.
- **AC-003 (REQ-004, REQ-005)**: Given `/warehouse` and alias `orders`, when
  rendering permanent locations under either flag value, then the derived path
  is `/warehouse/orders` and the output uses the language/format-specific API.
- **AC-004 (REQ-006)**: Given a physical Python staging identifier
  `orders__dbt_tmp_batch_1`, when rendered with `/warehouse`, then staging uses
  `/warehouse/orders__dbt_tmp_batch_1` while the permanent path remains unchanged.
- **AC-005 (CON-001, CON-004)**: Given a caller whose Jinja path renders the
  helper, when no root is supplied and the flag is true, then compilation fails
  independently of later Python runtime branch selection.
- **AC-006 (CON-002, CON-003)**: Given an empty string or root ending in `/`,
  when the helper derives a path, then existing concatenation semantics remain;
  no URI, writability, or normalization guarantees are claimed.

## 6. Test Automation Strategy

- **Test levels and frameworks**: Existing pytest/unittest and Jinja macro
  rendering for both helpers; functional physical-staging location tests and
  opt-in live streaming creation where appropriate.
- **Test data management**: In-memory config for render tests and isolated
  temporary storage/catalog entries for live tests. Preserve fixture cleanup.
- **CI/CD integration**: Existing unit workflow and configured local session
  functional profiles; no additional service or tool is required.
- **Coverage requirements**: Both flag values, missing/null/configured/blank
  roots, SQL/Python APIs, Iceberg/non-Iceberg writers, permanent/staging paths,
  and helper-rendering boundaries. No numeric threshold is imposed.
- **Performance testing**: Helpers perform no storage probes; validate emitted
  code rather than introducing I/O-based path checks.

From `dbt-spark`:

```bash
hatch run unit-tests -- tests/unit/test_macros.py tests/unit/test_streaming_materialization.py
```

Existing macro tests cover location derivation and flag-disabled defaults.
Changes to enforcement must additionally cover the flag-enabled failure matrix;
the command above does not imply that every specified edge case already has a test.

## 7. Rationale & Context

Explicit locations avoid relying on deployment-dependent managed-table defaults.
The opt-out preserves managed placement when intended. The flag's actual
boundary is location-helper rendering, so a broader claim that every model is
validated globally would misrepresent behavior. Staging isolation remains a
separate contract because temporary writes must not share the permanent path.

## 8. Dependencies & External Integrations

### External Systems
- **EXT-001**: Spark table provider/catalog supporting configured table locations.
### Third-Party Services
- **SVC-001**: Storage service selected by the deployment, if not local storage.
### Infrastructure Dependencies
- **INF-001**: Storage and catalog permissions necessary for the derived location.
### Data Dependencies
- **DAT-001**: Configured root, model alias, and optional final staging identifier.
### Technology Platform Dependencies
- **PLT-001**: Existing dbt configuration validation, Jinja compilation, and
  Spark SQL/WriterV2 location interfaces.
### Compliance Dependencies
- **COM-001**: Storage governance/access rules are deployment responsibilities;
  this flag does not certify or enforce allowed locations.

## 9. Examples & Edge Cases

```yaml
# Allow engine-managed placement when no root is supplied.
flags:
  require_location_root: false
```

A supplied root still applies in this mode. With root `/warehouse/` and alias
`orders`, concatenation produces `/warehouse//orders`; helpers do not normalize
it. An empty string is different from null in current behavior. Missing-root
errors are compiler errors, not a successful fallback to managed storage when
the flag is enabled.

## 10. Validation Criteria

Check registered default, helper conditions, exact output APIs, and actionable
errors against source. Preserve existing permanent/staging derivation and do
not add implied path validation or relocation. Documentation-only creation does
not assert that any live storage validation was performed.

## 11. Related Specifications / Further Reading

- [Flag registration](../dbt-spark/src/dbt/adapters/spark/impl.py)
- [Location helpers](../dbt-spark/src/dbt/include/spark/macros/adapters.sql)
- [Python table writer](../dbt-spark/src/dbt/include/spark/macros/materializations/table.sql)
- [Location macro tests](../dbt-spark/tests/unit/test_macros.py)
- [Physical staging functional tests](../dbt-spark/tests/functional/adapter/test_python_incremental_staging_location.py)
- [Python staging-location specification](../dbt-spark/specs/spec-design-spark-python-incremental-staging-location.md)
- [Iceberg write-order specification](../dbt-spark/specs/spec-design-iceberg-write-order.md)
- [Streaming specification](../dbt-spark/specs/spec-design-spark-streaming-models.md)
