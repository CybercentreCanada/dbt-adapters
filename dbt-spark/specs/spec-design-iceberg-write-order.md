---
title: dbt-spark Iceberg Write Order
version: "1.1"
date_created: 2026-11-03
last_updated: 2026-11-03
tags: [design, dbt-spark, iceberg, spark, sql, python]
---

# Introduction

This specification records the confirmed design for configuring and synchronizing
Apache Iceberg write order in dbt-spark. It is self-contained and includes the
decisions reached during the design interview, implementation integration points,
failure contracts, compatibility boundaries, and required acceptance evidence.

The design and implementation were authorized on 2026-11-03. The implementation
and runtime validation evidence are recorded below. Certification is limited to
the tested platform and catalog; source inspection alone is not certification.

## 1. Purpose & Scope

Enable batch SQL and Python models using Iceberg to declare `write_order`. Capture
the current table order, compare it with the desired order, and change the table
policy when necessary.

The implementation scope is the `dbt-spark` package only. Supported materializations
are `table` and `incremental`, including initial creation, ordinary incremental
runs, full refresh, and batch microbatch execution. Internal staging relations
are not user-facing targets and must not inherit the target's order.

Initial certification targets Apache Spark 4.0.x and Iceberg table format versions
2 and 3. Other Spark 4.x minor lines require separate certification with matching
runtime artifacts. Older runtimes must retain existing behavior when the feature
is not configured. The package-wide minimum Spark requirement must not be raised.

Views, ephemeral models, streaming materializations, non-Iceberg tables, automatic
table-format upgrades, automatic historical-data rewrites, local write ordering,
sort transforms, and arbitrary SQL sort expressions are outside scope.

The intended audience is maintainers, implementation agents, and test authors.
Existing table schema, partitioning, properties, grants, documentation, hooks,
incremental strategies, and SQL/Python submission behavior must remain supported.

## 2. Definitions

| Term | Definition |
| --- | --- |
| dbt | Data build tool; executes configured model materializations. |
| Write order | Iceberg's table-level policy for sorting subsequent writes; not a query-result ordering guarantee. |
| Desired order | Normalized order derived from model configuration. |
| Current order | Order established by fresh, authoritative table metadata. |
| Unordered | Confirmed absence of an Iceberg sort policy; requested through `WRITE UNORDERED`. |
| Unknown order | Metadata does not establish the current policy; distinct from unordered. |
| Sort term | Column reference with direction and null placement. |
| Table format version | Iceberg metadata format version, here 2 or 3; not an Iceberg library release. |
| Runtime library | Iceberg integration artifact compatible with a particular Spark minor line and Scala binary version. |
| Target | User-facing relation materialized from a model. |
| Staging relation | Internal temporary view or physical table used to prepare model data. |
| Atomic replacement | Replacement publishes on successful commit; failure of the replacement write preserves the previous target. |
| Initial write | First data insertion into a newly created or replacement table. |
| Microbatch | Batch incremental execution split into batches; not a streaming materialization. |
| DDL | Data definition language, including CREATE and ALTER statements. |
| DML | Data manipulation language, including INSERT and MERGE statements. |
| CTAS / RTAS | CREATE TABLE AS SELECT / REPLACE TABLE AS SELECT. |
| SQL extensions | Iceberg's Spark SQL parser and execution extensions required for write-order DDL. |
| CI | Continuous integration. |

## 3. Requirements, Constraints & Guidelines

### Requirements

- **REQ-001**: Expose a single model configuration option, `write_order`, containing
  a list of column sort terms. Do not introduce a separate ordered/unordered toggle.
- **REQ-002**: Omission or an empty list means unordered on supported Iceberg
  targets, including removal of a policy configured by an earlier dbt run.
- **REQ-003**: Support quoted identifiers and optional `ASC`/`DESC` and
  `NULLS FIRST`/`NULLS LAST`. Reject transforms and arbitrary SQL expressions.
- **REQ-004**: Normalize omitted direction to `ASC`. Normalize omitted null placement
  to `NULLS FIRST` for ascending terms and `NULLS LAST` for descending terms.
  Ignore insignificant whitespace and keyword casing without changing identifier
  semantics. Preserve term sequence.
- **REQ-005**: Reject scalar strings, explicit null values, non-string entries,
  blank terms, malformed clauses, and duplicate column references with a
  model-specific compilation/configuration error before target mutation.
- **REQ-006**: Reject explicitly configured `write_order`, including `[]`, on
  unsupported runtimes/formats, non-Iceberg models, views, ephemeral models, and
  streaming materializations. Models without the option on unsupported surfaces
  retain their existing behavior.
- **REQ-007**: Apply the same public configuration and comparison semantics to SQL
  and Python models and both supported table format versions.
- **REQ-008**: For a new target, establish schema, configure ordered/unordered
  policy, and only then insert model data. Initial data writes must use that policy.
- **REQ-009**: For an existing incremental target, refresh metadata, compare order,
  and apply any required ALTER before target DML. Equivalent policies produce no
  write-order ALTER.
- **REQ-010**: Synchronize at most once per target per invocation through the existing
  microbatch metadata guard. A failed synchronization must not permit target writes.
- **REQ-011**: Preserve atomic replacement for existing targets, including full
  refresh. Apply the desired order after the replacement's initial write. This is
  the explicit exception to REQ-008 and REQ-009: the initial replacement write is
  not guaranteed to use the desired policy. After replacement, compare against
  freshly established replacement metadata, not the old target's order.
- **REQ-012**: Capture write order when describing an Iceberg table and store it as
  structured relation metadata. Do not depend solely on cached listing text.
- **REQ-013**: Use fresh DESCRIBE information before existing-target comparisons.
  If it cannot establish the policy, use a verified authoritative Iceberg metadata
  source. Fail clearly when neither source establishes it; never default unknown
  metadata to unordered.
- **REQ-014**: Emit `ALTER TABLE <target> WRITE ORDERED BY <terms>` for changed ordered
  policies and `ALTER TABLE <target> WRITE UNORDERED` when clearing ordering.
  Render explicit direction and null placement and correctly quote identifiers.
- **REQ-015**: Validate syntax locally and let Spark validate column existence and
  type eligibility before target mutation. Do not postpone semantic validation
  until a destructive operation. The safe preflight mechanism requires runtime
  verification.
- **REQ-016**: Do not rewrite existing data files merely because configuration
  changed. Do not promise ordered SELECT output or automatic physical-file
  reorganization.
- **REQ-017**: Document the configuration, compatibility prerequisites, defaults,
  lifecycle exception, failures, and examples. Add a changelog entry when the
  user-facing feature is implemented.

### Failure and security requirements

- **ERR-001**: A failure creating, configuring, or initially populating a new target
  must fail the run. Clean up only the new target owned by that operation; never
  drop a pre-existing target. Surface the original failure and any cleanup failure.
  Ownership must be established, not inferred solely from an earlier absence check.
- **ERR-002**: Failure of an atomic replacement write must preserve the old target.
- **ERR-003**: If replacement succeeds but order synchronization fails, fail dbt with
  an explicit explanation that replacement succeeded and ordering failed. Leave
  the replacement in place; do not attempt an unsafe rollback.
- **ERR-004**: Missing SQL extensions, incompatible runtime/format, inaccessible or
  unparseable metadata, and failed ALTER operations must produce actionable errors.
  No silent skips or success-shaped fallbacks are permitted.
- **SEC-001**: Do not install or enable runtime extensions dynamically. Do not
  expose credentials or commit credential-bearing test files.
- **SEC-002**: Parse configuration as bounded column terms and render validated,
  quoted identifiers. Never splice unchecked configuration or DESCRIBE presentation
  text directly into executable SQL.

### Constraints and patterns

- **CON-001**: Changes belong to dbt-spark; avoid unrelated base-package or adapter changes.
- **CON-002**: Require Spark 4.0.x for initial certification and Iceberg format v2/v3.
  Unsupported runtime/format plus explicit configuration must fail. No-option
  unsupported targets must not receive new ordering commands.
- **CON-003**: Respect existing model table-format configuration. Do not automatically
  upgrade existing tables or impose a new table-format default.
- **CON-004**: New-target schema creation, ALTER, and insertion are separate operations.
  Atomic publication to concurrent readers is not guaranteed for this path.
- **CON-005**: A custom catalog/JVM integration to configure ordering inside atomic
  staged writes is not required by the agreed replacement contract.
- **PAT-001**: Reuse table-property metadata diff/ALTER patterns where applicable,
  adapter macro dispatch, and the existing once-per-target invocation guard.
- **PAT-002**: Use typed internal metadata/configuration representations and existing
  error conventions. Decorate adapter methods with `@available` when called by Jinja.
- **GUD-001**: Prefer one shared parser/comparator for SQL, Python, metadata capture,
  and lifecycle synchronization rather than divergent language-specific semantics.

## 4. Interfaces & Data Contracts

### Model configuration

| Field | Accepted value | Meaning |
| --- | --- | --- |
| `write_order` omitted | No explicit value | Unordered on supported targets; unchanged behavior on unsupported surfaces. |
| `write_order: []` | Empty list | Explicit unordered request; compatibility validation still applies. |
| `write_order: [term, ...]` | Nonempty list of strings | Ordered policy, in list order. |
| Any other value | Invalid | Model-specific configuration error. |

The configuration reader must distinguish omission from an explicit empty value,
including when adapter configuration defaults are populated.

Each term consists of a Spark column reference, followed by optional direction,
followed by optional null placement. Backtick-quoted identifiers must retain their
meaning, including escaped backticks and embedded punctuation. Qualified references
must be parsed according to Spark's identifier rules rather than split naively.
Arbitrary expression syntax is not accepted.

### Logical internal contract

The following describes required information, not prescribed Python class names:

```text
SortTerm:
  column_reference: parsed identifier preserving Spark resolution semantics
  direction: ASC | DESC
  null_placement: FIRST | LAST

DesiredWriteOrder:
  explicitly_configured: boolean
  terms: ordered list[SortTerm]  # empty means unordered

CurrentWriteOrder:
  state: ordered | unordered | unknown
  terms: ordered list[SortTerm]  # populated only for ordered
  source: describe | authoritative_iceberg_metadata | unavailable
```

Unknown must not compare equal to an empty desired list. Equality requires identical
ordered normalized terms after safe identifier resolution. Duplicate detection
must use the same identifier semantics as comparison.

DESCRIBE capture must integrate with relation metadata without treating incomplete
listing information as a complete description. Exact storage classes, adapter
method names, fallback metadata access, and identifier resolution implementation
are implementation choices subject to live validation.

### Synchronization outcome

| Current | Desired | Required outcome |
| --- | --- | --- |
| Unordered | Unordered | No ALTER for an existing target. |
| Ordered A | Equivalent A | No ALTER. |
| Ordered A | Different B | ALTER to B. |
| Unordered | Ordered B | ALTER to B. |
| Ordered A | Unordered | ALTER WRITE UNORDERED. |
| Unknown | Any | Resolve authoritatively or fail before target DML. |

New targets must explicitly establish their requested policy before insertion;
the existing-target no-op optimization must not weaken that lifecycle requirement.

### Existing integration points

Paths are relative to this specification. These are investigated seams, not an
implementation mandate or a claim that current behavior already satisfies the spec.

| Surface | Existing location | Relevant work |
| --- | --- | --- |
| Adapter config and metadata | [impl.py](../src/dbt/adapters/spark/impl.py) | `SparkConfig`, DESCRIBE parsing, metadata fetch, macro-accessible helpers. |
| Relation metadata | [relation.py](../src/dbt/adapters/spark/relation.py) | Frozen relation currently lacks structured write-order state. |
| SQL creation | [adapters.sql](../src/dbt/include/spark/macros/adapters.sql) | CTAS currently combines table creation and initial writing. |
| Table/Python lifecycle | [table.sql](../src/dbt/include/spark/macros/materializations/table.sql) | Physical Python writes currently use DataFrameWriterV2 `create()`; replacement safety needs explicit wiring. |
| Incremental lifecycle | [incremental.sql](../src/dbt/include/spark/macros/materializations/incremental/incremental.sql) | Metadata sync before DML; full-refresh branches must meet replacement contract. |
| Property comparison | [tblproperties.py](../src/dbt/adapters/spark/relation_configs/tblproperties.py) | Existing normalized config diff pattern. |
| Property ALTER dispatch | [tblproperties.sql](../src/dbt/include/spark/macros/relations/tblproperties.sql) | Existing metadata synchronization pattern. |
| Write-order contracts and lifecycle | [write_order.py](../src/dbt/adapters/spark/relation_configs/write_order.py) | Standard-library-only parser, structured metadata, comparison, ownership checks, and lifecycle shared with generated Python jobs. |
| Write-order macro dispatch | [write_order.sql](../src/dbt/include/spark/macros/relations/write_order.sql) | Parse-time model validation and managed SQL materialization. |
| Model documentation | [README.md](../README.md) | Document feature and lifecycle boundaries. |

Preserve hooks, schema-change handling, grants, documentation, constraints,
partitioning, properties, and staging cleanup while wiring these surfaces.

## 5. Acceptance Criteria

- **AC-001 (REQ-001, REQ-007)**: Given a SQL or Python model on either supported
  format, when it configures the same sort list, then both languages derive the
  same normalized desired policy.
- **AC-002 (REQ-002, REQ-008)**: Given a new supported target with omitted or empty
  configuration, when created, then WRITE UNORDERED is established before insertion.
- **AC-003 (REQ-008)**: Given a new ordered target, when first populated, then
  authoritative metadata and lifecycle evidence show ordering was established
  before the data write.
- **AC-004 (REQ-009, REQ-014)**: Given an existing incremental target, when column
  sequence, direction, or null placement changes, then the required ALTER runs
  before target DML and authoritative metadata matches the desired order.
- **AC-005 (REQ-002, REQ-014)**: Given an ordered target, when the setting is removed
  or changed to `[]`, then the policy becomes unordered.
- **AC-006 (REQ-004, REQ-009)**: Given semantically equivalent configurations
  differing only in allowed normalization, when rerun, then no order ALTER occurs.
- **AC-007 (REQ-011, ERR-002)**: Given an existing target, when replacement writing
  fails, then its previous data and metadata remain accessible and unchanged.
- **AC-008 (REQ-011)**: Given successful replacement/full refresh, when the run
  completes, then the new target's final policy matches configuration. No assertion
  requires its initial replacement write to use that policy.
- **AC-009 (ERR-003)**: Given successful replacement followed by failed order ALTER,
  when dbt reports the result, then it fails explicitly and leaves the replacement
  present without claiming rollback.
- **AC-010 (ERR-001)**: Given a newly created target and a configuration/insertion
  failure, when cleanup runs, then only the operation-owned new table is removed;
  cleanup errors are surfaced alongside the initial failure.
- **AC-011 (REQ-005, REQ-015)**: Given invalid input or an invalid column reference,
  when validated, then a clear error occurs before target mutation.
- **AC-012 (REQ-006, CON-002)**: Given explicit configuration on an unsupported
  surface, when run, then it fails actionably; omission preserves existing behavior.
- **AC-013 (REQ-012, REQ-013)**: Given ordered, unordered, incomplete, redacted, or
  malformed DESCRIBE metadata, when inspected, then structured state is captured,
  resolved through a verified fallback, or rejected; unknown is never unordered.
- **AC-014 (REQ-010)**: Given multiple microbatches for one target in an invocation,
  when ordering differs, then synchronization occurs once before any target write.
- **AC-015 (REQ-007)**: Given physical or temporary internal staging, when model
  data is prepared, then target order is not applied to staging relations.
- **AC-016 (REQ-016, CON-003)**: Given a policy change, when synchronized, then no
  historical rewrite or table-format upgrade is initiated.
- **AC-017 (SEC-002)**: Given quoted identifiers containing spaces, commas, dots,
  or escaped backticks, when parsed, compared, and rendered, then their semantics
  are preserved without executing unintended SQL.
- **AC-018 (REQ-017, PAT-001, PAT-002)**: Given the implementation, when reviewed,
  then dispatch/decorators/guards are wired, related docs and changelog exist, and
  pre-existing materialization behavior is preserved.

## 6. Test Automation Strategy

- **Test levels**: Unit tests for validation, normalization, comparison, metadata
  parsing, compatibility gating, and errors; macro/rendered-Python tests for wiring;
  live integration tests for actual policy and lifecycle behavior.
- **Frameworks**: Existing pytest, dbt adapter fixtures, and Hatch environments.
  Reuse the local Spark test pattern where suitable; its investigated Spark 3.5.6 /
  Iceberg 1.5.0 pins are not sufficient to certify this feature.
- **Required live matrix**: SQL and Python; table and incremental; Iceberg formats
  v2 and v3; creation, repeat run, order change, clearing by omission and `[]`,
  equivalence, full refresh, and applicable failure contracts. Exercise supported
  incremental strategies and the microbatch guard without conflating streaming.
- **Test data management**: Use isolated catalog namespaces and operation-owned
  tables, deterministic data with multiple keys and nulls, and explicit cleanup.
  Keep credentials outside version control.
- **Failure injection**: Cover prewrite ALTER failure, insert failure, replacement
  write failure, post-replacement ALTER failure, metadata failure, and cleanup failure.
- **CI integration**: Wire focused tests into the existing dbt-spark automation.
  Record exact tested Spark/runtime pairs. Missing prerequisites must produce an
  explicit skip/blocker; skipped live tests are not certification evidence.
- **Coverage requirements**: Exercise every acceptance criterion and both sides
  of each synchronization decision. No arbitrary numerical coverage threshold is
  introduced by this specification.
- **Performance testing**: Verify no redundant order ALTER for equivalent policies
  and one synchronization per target/invocation. No throughput or latency threshold
  was agreed. Do not add historical rewrites or unbounded metadata scans.
- **Quality commands**: Run focused repository-configured tests first, then required
  `hatch run code-quality` and relevant unit tests from the dbt-spark directory.

Rendered DDL alone is insufficient. Use authoritative Iceberg sort metadata and
operation sequencing to verify the policy. Do not use SELECT output order as a
proxy for Iceberg write-order correctness.

## 7. Rationale & Context

One list option avoids contradictory ordered/unordered settings and expresses
column precedence directly. Strict validation prevents misplaced configuration
from silently doing nothing. Defaults are canonicalized so equivalent policies
do not cause repeated DDL.

The user initially requested ordering at creation for all writes. Investigation
found that the documented stock interface sets ordering through ALTER, while
Spark's atomic replacement writes into a staged table without a user ALTER hook.
The user explicitly chose to preserve atomic replacement and allow ordering to be
applied after its initial write. This exception supersedes the earlier universal
first-write requirement; it does not apply to new targets or ordinary incremental
writes. Non-atomic replacement and custom JVM/catalog integration were not selected.

New-table schema creation, ALTER, and insertion satisfy the remaining first-write
requirement but cannot promise atomic publication. Cleanup and explicit failures
are therefore part of the contract.

Fresh metadata is necessary because fast Iceberg relation listings can contain
empty information strings. In the inspected Spark 4.0.0 / Iceberg 1.10.0 source,
Iceberg exposes nonempty ordering as a synthetic `sort-order` property; Spark
DESCRIBE EXTENDED includes it in `Table Properties`. Unordered tables omit that
property. This omission means unordered only in a verified complete metadata
representation; cached, redacted, or otherwise incomplete output is not proof.

The display representation interpolates source names without quoting and has
transform-rendering asymmetries. It is not a general SQL round-trip format.
Structured metadata and a verified fallback avoid unsafe parsing assumptions.

The target changed during the interview from a broad Spark environment to Spark
4.x and Iceberg formats 2/3, then narrowed initial certification to Spark 4.0.x.
Iceberg library versions and table format versions must never be conflated.

## 8. Dependencies & External Integrations

### External Systems

- **EXT-001**: Iceberg-capable Spark catalog providing fresh table metadata,
  write-order ALTER, and atomic replacement for supported target operations.
  Catalog capabilities must be verified rather than assumed universal.

### Third-Party Services

- **SVC-001**: No new hosted service is required. Existing catalog/object-storage
  services may be reused under their existing authentication configuration.

### Infrastructure Dependencies

- **INF-001**: Live Spark execution environment, compatible Java/Scala runtime,
  Iceberg artifact availability, and isolated writable warehouse storage.
- **INF-002**: Preconfigured Iceberg SQL extensions. Missing extensions are an
  actionable failure, not a reason to install or restart services automatically.
- **INF-003**: CREATE/DROP permissions for isolated probe tables in catalog-managed
  storage and Spark read access to table metadata files. The authoritative fallback
  selects the latest metadata file from `metadata_log_entries`, reads it as whole
  text through a temporary view, and parses raw JSON without Spark schema inference.

### Data Dependencies

- **DAT-001**: Fresh schema, table format version, and authoritative sort-order
  metadata, including stable identifier resolution for comparison.
- **DAT-002**: Model configuration retaining explicit-presence information.

### Technology Platform Dependencies

- **PLT-001**: Initial feature certification is Spark 4.0.x with Iceberg formats
  v2/v3; other 4.x minor lines require independent certification.
- **PLT-002**: A matching Iceberg runtime supporting both formats and ordering DDL.
  The source-verified baseline is Spark 4.0.0 with Iceberg library 1.10.0 using
  `org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0`. This is a concrete
  validation candidate, not a mandate to pin all deployments to that patch/release.
- **PLT-003**: Existing dbt-spark model configuration, macro dispatch, SQL/Python
  submission, relation metadata, and testing infrastructure.

### Compliance Dependencies

- **COM-001**: Existing repository credential-handling rules apply. No additional
  regulatory requirement was introduced by this design.

## 9. Examples & Edge Cases

### SQL model

```sql
{{ config(
    materialized='incremental',
    file_format='iceberg',
    write_order=['customer_id ASC NULLS LAST', 'total DESC']
) }}

select customer_id, total, order_date
from {{ ref('source_orders') }}
```

For an existing target with a different policy, the normalized ordering DDL is:

```sql
ALTER TABLE db.orders WRITE ORDERED BY
  customer_id ASC NULLS LAST, total DESC NULLS LAST;
```

Another requested configuration can produce:

```sql
ALTER TABLE db.orders WRITE ORDERED BY
  customer_id ASC NULLS LAST, order_date DESC NULLS LAST;
```

### Python model

```python
def model(dbt, session):
    dbt.config(
        materialized="table",
        file_format="iceberg",
        write_order=["customer_id ASC NULLS LAST", "total DESC"],
    )
    return dbt.ref("source_orders")
```

Both examples rely on the existing deployment's separate table-format configuration
to select v2 or v3; `write_order` does not select or upgrade that format.

### Clearing and equivalence

```yaml
write_order: []
```

Removing `write_order` entirely has the same desired policy on a supported target:

```sql
ALTER TABLE db.orders WRITE UNORDERED;
```

| Configuration or metadata | Interpretation |
| --- | --- |
| `['customer_id']` | Equivalent to `['customer_id ASC NULLS FIRST']`. |
| `['total DESC']` | Equivalent to `['total DESC NULLS LAST']`. |
| `['a', 'b']` versus `['b', 'a']` | Different order. |
| `['a ASC NULLS FIRST']` versus `['a ASC NULLS LAST']` | Different order. |
| `['`customer,id` DESC']` | One quoted column, not two terms. |
| `'customer_id ASC'` as a scalar | Invalid; a list is required. |
| `[null]`, `['']`, explicit null | Invalid. |
| `['customer_id', 'customer_id DESC']` | Invalid duplicate reference. |
| `['bucket(16, customer_id)']`, `['total + 1 DESC']` | Unsupported expressions. |
| Missing `sort-order` in verified complete metadata | Unordered. |
| Missing order in empty cached listing text | Unknown; fetch authoritative metadata. |
| Replacement succeeds and ALTER fails | Failed dbt run; replacement remains present. |
| Supported runtime, existing v1 table, explicit `[]` | Fail; do not upgrade format. |
| Older runtime, omitted option | Preserve existing behavior; no new ordering DDL. |

## 10. Validation Criteria

Compliance requires:

1. All requirements and AC-001 through AC-018 are covered by passing tests.
2. Live certification evidence identifies exact Spark, Iceberg runtime, catalog,
   and table-format versions; both v2 and v3 are exercised.
3. New-target and incremental ordering is verified before data writes; replacement
   tests use the documented post-initial-write exception.
4. Failure tests demonstrate ownership-safe cleanup and atomic replacement safety.
5. Metadata tests demonstrate correct DESCRIBE capture, safe identifier handling,
   verified fallback, and explicit failure for unresolved state.
6. Supported SQL/Python paths, strategies, staging, hooks, grants, documentation,
   partitioning, and properties retain their intended behavior.
7. Unsupported-runtime no-option behavior remains unchanged.
8. Related user documentation, changelog, code quality, and focused unit/live tests
   pass. Documentation-only creation of this specification needs no code tests.

### Implementation and validation evidence

The concrete live environment is Spark 4.0.3, Scala 2.13, Iceberg library 1.10.0,
Python 3.12.3, Java 17, and an Iceberg Hadoop catalog. Both table formats v2 and v3
are exercised. This evidence does not certify other Spark minors, other catalogs,
or every additional feature of format v3.

- [Configuration and metadata unit tests](../tests/unit/test_write_order.py)
  cover invalid values, normalization, duplicate detection, explicit-presence
  semantics, compatibility gating, structured DESCRIBE capture, and unknown state.
- [Actual dbt parse tests](../tests/unit/test_write_order_parse.py) cover SQL/Python
  configuration retention and rejection on views, ephemeral models, and streaming.
- [Live lifecycle tests](../tests/unit/test_write_order_live.py) execute generated
  Python and actual `dbt run` table/incremental paths, changes, clearing, equivalence,
  full refresh, append/merge/overwrite, and concurrent microbatch runs.
- Live tests also exercise quoted and nested identifiers, policy-before-insertion
  evidence, replacement write failures, post-replacement ALTER failure, initial
  failures, cleanup failures, and changed ownership before cleanup.
- Validation uses an isolated empty probe and Iceberg's own ALTER analysis before
  mutating a target. New-target cleanup verifies an operation-specific ownership
  property; it refuses to drop a table whose ownership marker changed.
- Authoritative metadata is read as raw whole text. Spark JSON schema inference
  was observed to coerce mixed primitive/struct type representations and lose
  nested field IDs; the implementation does not use that inference path.
- Frozen relations are updated using dataclass replacement. Managed targets
  returned to dbt's cache include their Iceberg classification and write order;
  later microbatches must not fall back to legacy partition-overwrite syntax.
- Python `statement('main')` remains directly inside each materialization:
  dbt's submission call-stack validation rejects an intermediate dispatched
  materialization helper. SQL records the real data-write response as `main`.
- Existing format v3 is retained for replacement/probe construction when model
  table-format properties are omitted; no automatic format upgrade is introduced.

The dedicated command is `hatch run write-order-tests:test` from the dbt-spark
directory. It is wired into Linux dbt-spark unit-test CI. `DBT_ICEBERG_JAR` can select
a preinstalled matching runtime instead of Maven resolution. Missing prerequisites
or skipped live tests do not constitute certification.

The confirmed contract remains authoritative. A conflict or proposed extension
to another catalog/runtime requires explicit approval and additional evidence.

## 11. Related Specifications / Further Reading

### Repository references

- [dbt-spark README](../README.md)
- [dbt-spark Hatch environments](../hatch.toml)
- [Macro unit tests](../tests/unit/test_macros.py)
- [Iceberg table-property lifecycle tests](../tests/functional/adapter/tblproperties/test_iceberg_tblproperties.py)
- [Existing local live Spark fixture](../tests/unit/test_streaming_materialization_live.py)

No existing dbt-spark specification was found during creation of this document.

### Official documentation and source evidence

Latest documentation is contextual; tagged source below establishes the concrete
investigated baseline. Neither substitutes for live tests.

- [Iceberg Spark DDL: write ordering](https://iceberg.apache.org/docs/latest/spark-ddl/#alter-table--write-ordered-by)
- [Iceberg Spark writes: distribution modes](https://iceberg.apache.org/docs/latest/spark-writes/#writing-distribution-modes)
- [Iceberg specification: sorting](https://iceberg.apache.org/spec/#sorting)
- [Spark SQL ORDER BY](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-orderby.html)
- [Iceberg 1.10.0 Spark runtime artifact](https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.0_2.13/1.10.0/iceberg-spark-runtime-4.0_2.13-1.10.0.pom)
- [Iceberg 1.10.0 table format support/defaults](https://github.com/apache/iceberg/blob/apache-iceberg-1.10.0/core/src/main/java/org/apache/iceberg/TableMetadata.java#L56-L75)
- [Spark 4.0 Iceberg parser ordering defaults](https://github.com/apache/iceberg/blob/apache-iceberg-1.10.0/spark/v4.0/spark-extensions/src/main/scala/org/apache/spark/sql/catalyst/parser/extensions/IcebergSqlExtensionsAstBuilder.scala#L254-L262)
- [Iceberg synthetic sort-order property](https://github.com/apache/iceberg/blob/apache-iceberg-1.10.0/spark/v4.0/spark/src/main/java/org/apache/iceberg/spark/source/SparkTable.java#L252-L254)
- [Spark 4.0 DESCRIBE property rendering](https://github.com/apache/spark/blob/v4.0.0/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/v2/DescribeTableExec.scala#L70-L76)
- [Iceberg sort-order display rendering](https://github.com/apache/iceberg/blob/apache-iceberg-1.10.0/spark/v4.0/spark/src/main/java/org/apache/iceberg/spark/Spark3Util.java#L994-L1062)
- [Iceberg staged catalog replacement](https://github.com/apache/iceberg/blob/apache-iceberg-1.10.0/spark/v4.0/spark/src/main/java/org/apache/iceberg/spark/SparkCatalog.java#L257-L305)
- [Spark atomic replacement execution](https://github.com/apache/spark/blob/v4.0.0/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/v2/WriteToDataSourceV2Exec.scala#L190-L225)
- [Spark Python DataFrameWriterV2 replacement delegation](https://github.com/apache/spark/blob/v4.0.0/python/pyspark/sql/readwriter.py#L2459-L2481)
