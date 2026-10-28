# Schema Contracts for Spark Python Models

## Context

Spark Python models cannot currently use native `contract.enforced: true`.
Supporting this configuration requires changes at two distinct layers:
dbt-core must permit Python contracts during parsing, and dbt-spark must
validate the schema of the DataFrame returned by the model before writing it.
Enabling the configuration alone would not provide enforcement.

### Verified core prerequisite

In the inspected dbt-core `1.13.0a1` installation,
`dbt.parser.schemas.ModelPatchParser.patch_constraints()` calls
`_validate_constraint_prerequisites()` when contract enforcement is enabled.
That method explicitly rejects languages other than SQL, even when the model
declares columns without row-level constraints.

A focused execution of the actual prerequisite method with a model-shaped
Python input produced:

```text
Parsing Error
  Contract enforcement failed for: (models/example.py)
  Language Error: Expected 'sql' but found 'python'
```

The equivalent SQL input passed. This verifies the parser restriction in that
installation; it is not a full `dbt parse` or `dbt run` reproduction, nor
verification of every core version allowed by the adapter's dependency range.

The inspected core also requires contracted incremental models to set
`on_schema_change` to `fail` or `append_new_columns`. Adapter support must
preserve this validation rather than silently overriding the configuration.

### Current adapter behavior

- [spark__create_table_as](../../dbt-spark/src/dbt/include/spark/macros/adapters.sql)
  validates contracts only in its non-temporary SQL creation branch, using
  `get_assert_columns_equivalent()` and `get_select_subquery()`. The latter
  projects SQL output into declared column order. The Python branch delegates
  to `py_write_table()` without equivalent schema checks.
- [py_write_table](../../dbt-spark/src/dbt/include/spark/macros/materializations/table.sql)
  executes the Python model and converts supported pandas, pandas-on-Spark,
  and Koalas outputs into a Spark DataFrame. It then creates a temporary view
  or writes a physical table through DataFrameWriterV2. The point after
  conversion and before either publication path is the appropriate place to
  validate the actual result schema.
- The shared
  [SQL contract validator](../../dbt-adapters/src/dbt/include/global_project/macros/relations/column/columns_spec_ddl.sql)
  discovers schemas through SQL queries. It cannot inspect compiled Python
  source as if that source were a SQL query.
- The
  [incremental materialization](../../dbt-spark/src/dbt/include/spark/macros/materializations/incremental/incremental.sql)
  has separate initial-create, full-refresh, and existing-target paths.
  Session-based Python submissions use temporary views for staging;
  other submission methods create physical staging tables.
- `spark__persist_constraints()` in the
  [adapter macros](../../dbt-spark/src/dbt/include/spark/macros/adapters.sql)
  applies constraints only for Delta with contract enforcement enabled.
  It handles column-level NOT NULL and model-level CHECK under its existing
  conditions, including skipping CHECK creation on ordinary incremental runs.
  There is no equivalent Iceberg persistence path. The
  [adapter constraint declarations](../../dbt-spark/src/dbt/adapters/spark/impl.py)
  mark constraint kinds as `NOT_ENFORCED` globally; they must not be changed
  to universal enforcement claims merely to enable Python schema validation.
- The session-based
  [Python submission helper](../../dbt-spark/src/dbt/adapters/spark/python_submissions.py)
  chains execution exceptions but replaces the outer message with
  "The Python model failed". Remote helpers report job failures differently.
  Contract mismatch details need to remain visible across both paths.

Schema contracts and row-level constraints are separate concerns. Matching
column names and data types does not establish that rows satisfy NOT NULL,
CHECK, uniqueness, primary-key, or foreign-key constraints.

## Options

- Wait for coordinated dbt-core and adapter support.
- Prepare adapter-side runtime validation while documenting the core blocker.
- Bypass core validation or substitute an adapter-specific setting.

### Wait for coordinated dbt-core and adapter support

#### Pro's

- Native parsing and execution behavior can be released and tested together.
- Core can allow Python contracts only for adapters and materializations that
  explicitly support them, without enabling unenforced contracts elsewhere.

#### Con's

- Requires work outside this repository.
- Delays adapter preparation until the core extension mechanism is agreed.

### Prepare adapter-side runtime validation

#### Pro's

- Keeps implementation work within this repository.
- Allows schema comparison, staging, and diagnostics to be tested independently.
- Provides an explicit path to native support once compatible core support exists.

#### Con's

- Does not make `contract.enforced: true` usable with the verified core parser.
- Requires careful separation between adapter readiness and end-to-end support.
- Generated code must run in remote Spark environments without assuming that
  dbt itself is installed there.

### Bypass core validation or substitute an adapter-specific setting

#### Pro's

- Could offer a separate runtime check without waiting for native core support.

#### Con's

- Monkey-patching core, spoofing model language, or disabling enforcement hides
  the compatibility boundary and is not a supported native contract implementation.
- An adapter-specific setting introduces different semantics and cannot honestly
  be presented as support for `contract.enforced: true`.

## Decision

#### Selected: Prepare adapter-side runtime validation, with native support blocked by core

Keep any subsequent implementation within this repository. Do not patch installed
dependencies, monkey-patch the parser, spoof the model language, or turn enforcement
off to avoid the restriction. This ADR records an architectural direction and
implementation requirements; it does not implement or announce working support.

Native activation still requires a compatible dbt-core change. A future coordinated
change should restrict Python contract acceptance to supporting adapters and
materializations. The current
[adapter capability enum](../../dbt-adapters/src/dbt/adapters/capability.py)
has no Python-contract capability; the extension mechanism remains to be agreed
with core. Do not invent a minimum compatible core version before it exists and
has been tested.

### Scope and schema semantics

- Cover Python `table` and `incremental` models, including initial creation,
  full refresh, subsequent incremental runs, and microbatch where already supported.
- Exclude streaming, Python views, new Iceberg constraint persistence, new
  uniqueness/primary-key/foreign-key guarantees, and automatic type coercion.
- Require declared columns with data types. Reject missing, extra,
  duplicate/ambiguous names and mismatched types before publishing model output.
- Resolve declared SQL type aliases with Spark rather than maintaining string
  replacements. Retain decimal precision/scale and nested array/map/struct types.
- Accept a different top-level output column order, then select validated columns
  into YAML order. Never silently drop extra fields or cast incompatible output.
- Specify case and quoting behavior against the existing SQL contract path and
  Spark's case-sensitivity setting. Do not introduce unconditional case folding.
  Nested field order remains part of type compatibility.
- Treat schema nullability metadata separately from row-level enforcement.
  A nullable DataFrame field does not prove that any returned row is null.
- Preserve existing Delta-specific constraint persistence and limitations.
  Iceberg schema validation must not imply new NOT NULL or CHECK enforcement.
- Preserve non-contracted Python behavior and existing SQL contract behavior.

### Required implementation

1. **Generate a shared, self-contained validator.** Call it from
   `py_write_table()` after DataFrame conversion, gated on enforcement.
   Reuse [get_empty_schema_sql](../../dbt-adapters/src/dbt/include/global_project/macros/adapters/columns.sql)
   to construct the declared typed query and resolve its schema in the executing
   Spark session. Compare it with `df.schema` without collecting model rows.
   Safely serialize generated metadata/query strings. Compare normalized types
   without treating nullable metadata as a row constraint, and emit explicit
   expected/actual differences. Quote literal identifiers correctly when
   projecting the validated result, including names containing dots or backticks.
2. **Validate every publication path.** Check before temporary-view registration
   and physical-table writes. On incremental runs, validate each batch before
   schema-change handling and append/merge/overwrite can coerce or discard
   differences. Preserve existing `on_schema_change` rules, session-specific
   staging behavior, and microbatch-specific relation names.
3. **Handle target replacement and cleanup.** The current Python writer calls
   `writer.create()`, but some materialization branches retain existing
   Delta/Iceberg targets expecting replacement. Adding only a schema check would
   leave this lifecycle mismatch unresolved. For contracted rebuilds, prepare
   and validate output before destructive replacement, using supported
   format-specific replacement semantics and existing staging mechanisms.
   Clean up physical staging objects on failure as well as success. Do not
   promise rollback across arbitrary hooks, model side effects, provider failures,
   or post-write constraint DDL.
4. **Preserve useful errors across submissions.** Keep expected/actual schema
   diagnostics visible for session-based and remote submissions and preserve
   exception chaining. A remote runtime exception is not automatically a host-side
   dbt `ContractError`; actionable runtime diagnostics do not require pretending
   that such transport already exists.
5. **Document compatibility accurately.** Explain schema versus row-constraint
   guarantees, incremental settings, and the core prerequisite in the
   [Spark README](../../dbt-spark/README.md) when implementation is undertaken.
   Label preparation as internal work rather than a shipped user-facing feature.

### Verification and acceptance

Extend the existing
[macro unit tests](../../dbt-spark/tests/unit/test_macros.py),
[Python model functional tests](../../dbt-spark/tests/functional/adapter/test_python_model.py),
and [SQL constraint regressions](../../dbt-spark/tests/functional/adapter/test_constraints.py)
with focused runtime coverage:

- Matching schemas, empty DataFrames, reordered columns, Spark type aliases,
  decimal precision/scale, and nested types.
- Missing/extra/duplicate fields, incorrect types, missing declared data types,
  case sensitivity, and literal quoted identifiers.
- Supported pandas-compatible results, validated after conversion.
- Initial runs, repeat table runs, full refresh, append, merge,
  insert_overwrite, and supported microbatch execution.
- Temporary-view and physical staging paths, cleanup on failure, and actionable
  local/remote errors.
- No publication on schema mismatch, unchanged existing incremental targets on
  mismatch, and validation before destructive rebuild replacement.
- Non-contracted Python and existing SQL behavior, plus unchanged Delta
  constraint persistence.

Adapter-only tests establish preparation, not native feature completion.
Final acceptance requires real `dbt parse` and `dbt run` executions using a
compatible core, including failing contracts before target mutation. Until then,
native acceptance is externally blocked.

## Consequences

- [+] Schema validation has a clear runtime insertion point shared by Python
  table and incremental execution.
- [+] Existing SQL contracts and format-specific constraint semantics remain
  the compatibility baseline.
- [+] Strict comparison avoids treating Spark writer coercion as contract
  enforcement, and provides explicit mismatch diagnostics.
- [-] Repository-only implementation cannot remove the verified core parser
  restriction or deliver usable native support by itself.
- [-] Contracted rebuilds require lifecycle and staging work beyond simply
  comparing schemas.
- [-] Remote execution and supported Spark versions require dedicated tests;
  successful local schema tests do not certify every submission method or runtime.
- [-] Iceberg row-level constraint enforcement and streaming contracts remain
  separate, unimplemented work.
