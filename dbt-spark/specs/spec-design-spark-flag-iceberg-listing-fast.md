---
title: dbt-spark iceberg_listing_fast Behavior Flag
version: "1.0"
date_created: 2026-11-04
last_updated: 2026-11-04
tags: [design, dbt-spark, behavior-flags, iceberg, catalogs, performance]
---

# Introduction

This specification defines the existing `iceberg_listing_fast` flag: an
optimization that discovers persistent relations in confirmed Iceberg-only
Spark session catalogs without per-relation describes.

## 1. Purpose & Scope

The audience is adapter maintainers, implementation agents, and test authors.
Scope covers eligibility, catalog-class detection/cache behavior, listing
queries, relation identity, metadata deferral, and errors. It does not authorize
provider inference from model configuration or extend the fast path to remote
connections or mixed-provider catalogs.

## 2. Definitions

| Term | Definition |
| --- | --- |
| Iceberg | Apache Iceberg, a table format with Spark catalog integration. |
| Session connection | A connection using the adapter's local Spark session method. |
| Iceberg-only catalog | A catalog whose detected class is exactly `org.apache.iceberg.spark.SparkCatalog`. |
| Mixed-provider catalog | A catalog that may contain tables with multiple providers, including `SparkSessionCatalog`. |
| Presentation metadata | Relation information text used for schema/comment/provider discovery outside the fast listing. |
| JVM | Java Virtual Machine backing the Spark session. |

## 3. Requirements, Constraints & Guidelines

- **REQ-001**: Register `iceberg_listing_fast` with default `true`.
- **REQ-002**: Select fast listing only when this flag and
  `use_v2_relation_listing` are true and the catalog is confirmed Iceberg-only
  through a session connection.
- **REQ-003**: Detect the catalog class from `spark.sql.catalog.<catalog>` when
  present; otherwise inspect the live JVM catalog class. When no catalog is
  supplied, resolve the session's current catalog on each detection call.
- **REQ-004**: Cache positive and negative class determinations by Spark session
  object and resolved catalog, under a lock. Concurrent requests for the same
  pair shall not duplicate class lookup.
- **REQ-005**: Unknown class results and handled detection errors shall retain
  regular listing. Log detection failures at debug level and do not cache them
  as definitive results; subsequent calls may retry.
- **REQ-006**: Successful fast listing shall execute exactly two namespace
  queries: `SHOW TABLES` and `SHOW VIEWS`, regardless of relation count, and shall
  execute no per-relation `DESCRIBE`.
- **REQ-007**: Exclude temporary rows from both results. Return each persistent
  view as a view, not as a duplicate table. Persistent tables shall be marked
  `is_iceberg=true`; views shall be `is_iceberg=false`. Both have
  `is_delta=false`, `is_hudi=false`, and empty `information`.
- **REQ-008**: Preserve input relation database/schema and quote/include
  policies, while ensuring the returned relation includes its identifier.
- **REQ-009**: Fetch column metadata through existing on-demand column/catalog
  paths when requested; empty presentation metadata shall not mean no columns.
- **REQ-010**: Recognized missing-schema errors in either listing query shall
  return `[]`. Other fast-listing errors, including permissions and unsupported
  view listing, shall propagate without automatic retry through regular listing.
- **CON-001**: Never infer Iceberg-only status from model `file_format`.
  `SparkSessionCatalog`, custom catalog classes, and remote connections are
  ineligible.
- **CON-002**: The two-query guarantee excludes eligibility lookup, later column
  metadata reads, and unrelated adapter work.
- **CON-003**: A confirmed class remains cached for that session/catalog pair;
  this contract does not promise live invalidation if its class changes.
- **GUD-001**: Preserve error visibility; eligibility failure and query execution
  failure have deliberately different fallback behavior.

## 4. Interfaces & Data Contracts

```yaml
flags:
  use_v2_relation_listing: true
  iceberg_listing_fast: true
```

```text
SHOW TABLES IN <schema_relation> LIKE '*'
SHOW VIEWS IN <schema_relation> LIKE '*'
row positions: [namespace, relation_name, is_temporary]
```

View/table deduplication uses the persistent rows' `(namespace, relation_name)`
identity. Returned database/schema come from `schema_relation`, not from
temporary-view namespaces. The selected path is:

| Configuration / eligibility | Path |
| --- | --- |
| V2 preference false | V1-first regular listing. |
| V2 true, fast false | V2-first regular listing. |
| Both true, remote/unknown/mixed catalog | V2-first regular listing. |
| Both true, confirmed session Iceberg-only catalog | Fast listing. |

To disable only the optimization, set `iceberg_listing_fast: false`.

## 5. Acceptance Criteria

- **AC-001 (REQ-001, REQ-002, CON-001)**: Given each dispatch/eligibility case,
  when listing, then only eligible session catalogs use the fast path.
- **AC-002 (REQ-003, REQ-004)**: Given repeated or concurrent lookup of a known
  session/catalog pair, when detecting its class, then class lookup occurs once;
  a new session or catalog uses a separate cached result.
- **AC-003 (REQ-003, REQ-005)**: Given no configured class, when detection runs,
  then the JVM fallback is used; given a handled detection failure, regular
  listing remains available and a later detection may retry.
- **AC-004 (REQ-006, CON-002)**: Given 0, 1, or 1000 table rows, when successful
  fast listing is invoked, then exactly two listing queries and zero
  per-relation describes occur.
- **AC-005 (REQ-007)**: Given tables, persistent views, and colliding temporary
  view names, when listing, then temporary rows are excluded and each persistent
  view appears once with correct provider/type flags.
- **AC-006 (REQ-008, REQ-009)**: Given quoted identifiers and empty presentation
  metadata, when relations and columns are requested, then identity/rendering
  remain correct and actual columns are fetched.
- **AC-007 (REQ-010)**: Given a missing schema, when either listing query fails,
  then return `[]`; given any other listing failure, propagate it without
  success-shaped empty data or regular-listing fallback.

## 6. Test Automation Strategy

- **Test levels and frameworks**: Existing pytest/unittest mocks for dispatch,
  SQL count, output flags, cache concurrency/isolation, on-demand columns, and
  errors; opt-in functional comparison against regular listing.
- **Test data management**: Mock row sets and session objects; live tests create
  and clean up isolated persistent tables/views and temporary view collisions.
- **CI/CD integration**: Unit tests in the existing workflow; live comparison
  requires `DBT_RUN_ICEBERG_LISTING_TESTS=1` and persistent-view support.
- **Coverage requirements**: All eligibility gates, both cached outcomes,
  uncached failures, concurrent misses, current-catalog changes, temporary rows,
  deduplication, and both query failures. No numeric threshold is imposed.
- **Performance testing**: Assert exact query counts for 0, 1, and 1000 tables,
  not only elapsed time.

From `dbt-spark`:

```bash
hatch run unit-tests -- tests/unit/test_adapter.py tests/unit/test_session_correctness.py
```

## 7. Rationale & Context

Confirmed Iceberg-only catalogs permit table provider classification without
describing each table. Two namespace queries reduce listing round trips while
retaining accurate persistent view identities. Restricting eligibility avoids
misclassifying mixed-provider tables. Deferred metadata keeps discovery cheap
without dropping column information needed by dbt documentation.

## 8. Dependencies & External Integrations

### External Systems
- **EXT-001**: Iceberg `SparkCatalog` with `SHOW TABLES` and `SHOW VIEWS` support.
### Third-Party Services
- **SVC-001**: None beyond the configured catalog and Spark deployment.
### Infrastructure Dependencies
- **INF-001**: Live session/JVM access and namespace listing permissions.
### Data Dependencies
- **DAT-001**: Catalog class identity and table/view listing rows with temporary flags.
### Technology Platform Dependencies
- **PLT-001**: Spark session connection, existing thread-safe cache, and dbt relations.
### Compliance Dependencies
- **COM-001**: Metadata authorization errors remain visible; no access bypass is defined.

## 9. Examples & Edge Cases

```yaml
# Retain v2 preference but restore per-relation metadata discovery.
flags:
  iceberg_listing_fast: false
```

`org.apache.iceberg.spark.SparkSessionCatalog` is not eligible even if all
currently configured models use Iceberg. A persistent view also returned by
`SHOW TABLES` appears only as a view. An unsupported `SHOW VIEWS` error fails
the fast query; disabling the optimization is an explicit user choice.

## 10. Validation Criteria

Verify the exact catalog allowlist, defaults, cache key/locking, output flags,
query count, and error distinctions against source. Live equivalence tests must
compare database/schema/name/type/provider identities, not presentation metadata.
Do not claim remote support or successful live execution solely from mocked tests.

## 11. Related Specifications / Further Reading

- [V2 preference flag](spec-design-spark-flag-use-v2-relation-listing.md)
- [Adapter implementation](../dbt-spark/src/dbt/adapters/spark/impl.py)
- [Unit listing and detection tests](../dbt-spark/tests/unit/test_adapter.py)
- [Session correctness tests](../dbt-spark/tests/unit/test_session_correctness.py)
- [Live listing comparison](../dbt-spark/tests/functional/adapter/test_iceberg_listing.py)
- [Adapter correctness specification](../dbt-spark/specs/spec-design-spark-adapter-correctness.md)
