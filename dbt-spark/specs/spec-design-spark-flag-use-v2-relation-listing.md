---
title: dbt-spark use_v2_relation_listing Behavior Flag
version: "1.0"
date_created: 2026-11-04
last_updated: 2026-11-04
tags: [design, dbt-spark, behavior-flags, catalogs, metadata]
---

# Introduction

This specification defines the existing `use_v2_relation_listing` flag, which
selects the preferred Spark relation-discovery path and interacts with the
Iceberg fast-listing flag. It describes current fallback and error behavior.

## 1. Purpose & Scope

The audience is adapter maintainers, implementation agents, and test authors.
Scope is `SparkAdapter.list_relations_without_caching()`, its regular v1/v2
listing paths, and dispatch to fast listing. This flag does not change table
format versions, Spark connection methods, or query materialization.

## 2. Definitions

| Term | Definition |
| --- | --- |
| Relation | A catalog/database, schema, identifier, and table/view type. |
| V1 listing | `SHOW TABLE EXTENDED` with relation metadata in each result row. |
| V2 listing | `SHOW TABLES` followed by per-relation `DESCRIBE EXTENDED`. |
| Fast listing | Two namespace listing queries without per-relation describes for eligible Iceberg catalogs. |
| Unsupported-operation error | An error matching the implementation's supported fallback conditions. |
| Iceberg | Apache Iceberg, a Spark table format. |

## 3. Requirements, Constraints & Guidelines

- **REQ-001**: Register `use_v2_relation_listing` with default `true`.
- **REQ-002**: If true and fast listing is enabled for an eligible catalog, use
  the fast path. Otherwise use regular v2-first listing.
- **REQ-003**: If false, use v1-first listing and bypass fast-listing eligibility
  detection in the top-level dispatcher.
- **REQ-004**: In regular v2-first mode, fall back to v1 only when a
  `DbtRuntimeError` message contains case-insensitive `"not supported"` or
  case-sensitive `"UNSUPPORTED_"`. Log that fallback at debug level.
- **REQ-005**: In v1-first mode, fall back to regular v2 when the error contains
  `"SHOW TABLE EXTENDED is not supported for v2 tables"`. Other operational
  failures shall propagate unless they match established missing-object handling.
- **REQ-006**: Recognized missing-schema or missing-table/view errors at the
  regular listing wrapper boundaries shall return an empty list. A relation
  disappearing during per-relation describe shall be omitted while preserving
  other discovered relations.
- **REQ-007**: Permission, invalid-identifier, and runtime errors not eligible
  for fallback shall propagate; an empty describe result shall not fabricate
  a table with empty metadata.
- **REQ-008**: Preserve relation database/schema identity and quote/include
  policies when building results. Regular describes shall render relation
  components rather than interpolate an absent catalog as `"None"`.
- **CON-001**: V1/v2 describe listing mechanisms here are not Iceberg table-format
  version selectors.
- **CON-002**: Setting this flag false changes preference, not absolute
  prohibition: v2 may still be reached through v1's supported fallback.
- **GUD-001**: Keep fallback conditions narrower than arbitrary exception
  handling; follow the existing adapter correctness requirements.

## 4. Interfaces & Data Contracts

```yaml
flags:
  use_v2_relation_listing: false
```

```text
list_relations_without_caching(schema_relation: BaseRelation) -> list[BaseRelation]
```

| `use_v2_relation_listing` | `iceberg_listing_fast` | Eligible catalog | Selected path |
| --- | --- | --- | --- |
| false | Either | Any | V1 first. |
| true | false | Any | Regular v2 first. |
| true | true | No / unknown | Regular v2 first. |
| true | true | Yes | Fast listing. |

Regular v2 listing describes persistent views separately for confirmed
Iceberg-only catalogs, removes duplicate table/view entries by identifier, and
marks returned views as non-Iceberg. Regular relation metadata is used to
classify table/view type and provider.

## 5. Acceptance Criteria

- **AC-001 (REQ-001 through REQ-003)**: Given each dispatch-table combination,
  when listing a namespace, then exactly the selected listing path is called.
- **AC-002 (REQ-004)**: Given unsupported regular v2 listing, when v2 is preferred,
  then v1 is attempted once and fallback is logged.
- **AC-003 (REQ-005, CON-002)**: Given the specific unsupported-v2-tables error
  from v1, when the flag is false, then regular v2 is attempted.
- **AC-004 (REQ-006)**: Given recognized missing namespace/object errors at a
  wrapper, when listing, then the established empty result is returned; given
  only one described relation disappeared, other relations remain.
- **AC-005 (REQ-007)**: Given permission, malformed-name, or runtime errors, when
  no supported fallback condition matches, then the error propagates.
- **AC-006 (REQ-007)**: Given an empty describe result for an extant listing row,
  when building metadata, then an explicit error is raised.
- **AC-007 (REQ-008)**: Given absent catalog or quoted punctuation/backticks in
  identifiers, when describing and returning a relation, then rendering policies
  and the relation's identity are preserved.

## 6. Test Automation Strategy

- **Test levels and frameworks**: pytest/unittest and mocks for dispatch,
  metadata parsing, fallback, rendering, and errors; local Spark/Iceberg
  functional comparison of regular and fast relation identities.
- **Test data management**: Mock listing rows and describe results; isolated
  tables/views for live tests with fixture-managed cleanup.
- **CI/CD integration**: Existing unit workflow; live comparison remains opt-in
  with an Iceberg catalog that supports persistent views.
- **Coverage requirements**: All dispatch rows, both fallback directions,
  recognized missing objects, permission failures, empty describes, and quote
  policies. No numeric coverage target is added.
- **Performance testing**: No constant-query guarantee is made for regular v2;
  per-relation metadata queries are expected.

From `dbt-spark`:

```bash
hatch run unit-tests -- tests/unit/test_adapter.py tests/unit/test_session_correctness.py
```

## 7. Rationale & Context

Some Spark engines cannot execute `SHOW TABLE EXTENDED` on v2 tables. V2-first
listing supports those tables while retaining a narrow fallback for legacy
catalogs. The separate fast flag optimizes eligible catalogs without changing
the legacy preference control.

## 8. Dependencies & External Integrations

### External Systems
- **EXT-001**: Spark catalog exposing at least one supported listing mechanism.
### Third-Party Services
- **SVC-001**: No additional service beyond the configured Spark connection.
### Infrastructure Dependencies
- **INF-001**: Namespace listing and relation describe permissions.
### Data Dependencies
- **DAT-001**: Catalog listing rows and relation type/provider metadata.
### Technology Platform Dependencies
- **PLT-001**: Existing dbt relation types, macro dispatch, and adapter error handling.
### Compliance Dependencies
- **COM-001**: Deployment authorization policies apply; failures are not bypassed.

## 9. Examples & Edge Cases

```yaml
# Exercise regular v2 behavior without the Iceberg optimization.
flags:
  use_v2_relation_listing: true
  iceberg_listing_fast: false
```

Forcing v1 first does not make an Iceberg catalog v1-compatible. The specific
unsupported-v2-tables error still selects v2. A permission error does not justify
pretending the schema is empty.

## 10. Validation Criteria

Verify flag defaults, dispatch precedence, exact fallback message conditions,
missing-object normalization, and rendering against the implementation. Preserve
the existing correctness specification's error-reporting invariant. This
document records a contract, not certification of every remote Spark engine.

## 11. Related Specifications / Further Reading

- [Fast-listing flag](spec-design-spark-flag-iceberg-listing-fast.md)
- [Adapter implementation](../dbt-spark/src/dbt/adapters/spark/impl.py)
- [Adapter listing tests](../dbt-spark/tests/unit/test_adapter.py)
- [Session correctness tests](../dbt-spark/tests/unit/test_session_correctness.py)
- [Live listing comparison](../dbt-spark/tests/functional/adapter/test_iceberg_listing.py)
- [Adapter correctness specification](../dbt-spark/specs/spec-design-spark-adapter-correctness.md)
