<p align="center">
    <img
        src="https://raw.githubusercontent.com/dbt-labs/dbt/ec7dee39f793aa4f7dd3dae37282cc87664813e4/etc/dbt-logo-full.svg"
        alt="dbt logo"
        width="500"
    />
</p>

<p align="center">
    <a href="https://pypi.org/project/dbt-spark/">
        <img src="https://badge.fury.io/py/dbt-spark.svg" />
    </a>
    <a target="_blank" href="https://pypi.org/project/dbt-spark/" style="background:none">
        <img src="https://img.shields.io/pypi/pyversions/dbt-spark">
    </a>
    <a href="https://github.com/psf/black">
        <img src="https://img.shields.io/badge/code%20style-black-000000.svg" />
    </a>
    <a href="https://github.com/python/mypy">
        <img src="https://www.mypy-lang.org/static/mypy_badge.svg" />
    </a>
    <a href="https://pepy.tech/project/dbt-spark">
        <img src="https://static.pepy.tech/badge/dbt-spark/month" />
    </a>
</p>

# dbt

**[dbt](https://www.getdbt.com/)** enables data analysts and engineers to transform their data using the same practices that software engineers use to build applications.

dbt is the T in ELT. Organize, cleanse, denormalize, filter, rename, and pre-aggregate the raw data in your warehouse so that it's ready for analysis.

## dbt-spark

`dbt-spark` enables dbt to work with Apache Spark.
For more information on using dbt with Spark, consult [the docs](https://docs.getdbt.com/docs/profile-spark).

# Getting started

Review the repository [README.md](../README.md) as most of that information pertains to `dbt-spark`.

## Running locally

A `docker-compose` environment starts a Spark Thrift server and a Postgres database as a Hive Metastore backend.
Note: dbt-spark now supports Spark 3.3.2.

The following command starts two docker containers:

```sh
docker-compose up -d
```

It will take a bit of time for the instance to start, you can check the logs of the two containers.
If the instance doesn't start correctly, try the complete reset command listed below and then try start again.

Create a profile like this one:

```yaml
spark_testing:
  target: local
  outputs:
    local:
      type: spark
      method: thrift
      host: 127.0.0.1
      port: 10000
      user: dbt
      schema: analytics
      connect_retries: 5
      connect_timeout: 60
      retry_all: true
```

Connecting to the local spark instance:

* The Spark UI should be available at [http://localhost:4040/sqlserver/](http://localhost:4040/sqlserver/)
* The endpoint for SQL-based testing is at `http://localhost:10000` and can be referenced with the Hive or Spark JDBC drivers using connection string `jdbc:hive2://localhost:10000` and default credentials `dbt`:`dbt`

Note that the Hive metastore data is persisted under `./.hive-metastore/`, and the Spark-produced data under `./.spark-warehouse/`. To completely reset you environment run the following:

```sh
docker-compose down
rm -rf ./.hive-metastore/
rm -rf ./.spark-warehouse/
```

## Running Integration Tests

To run the integration tests via Dagger (containerized Spark + Postgres metastore):

```sh
cd dbt-spark
hatch run setup
hatch run integration-tests
```

If your environment uses custom or corporate root certificates (e.g., behind a
proxy or on a government network), export the following before running:

```sh
export SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
export REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt
export PIP_CERT=/etc/ssl/certs/ca-certificates.crt
hatch run integration-tests
```

This ensures pip, git+https fetches, and Python HTTP clients use the system
trust store instead of the bundled `certifi` CA bundle.

## Additional Configuration for ODBC

The ODBC connection method (and the unit test suite, which exercises the ODBC
profile fixture) requires the `unixODBC` system library (`libodbc.so.2`) in
addition to the `dbt-spark[ODBC]` Python extra. Install it before running
`hatch run unit-tests`:

**MacOS:**
```sh
brew install unixodbc
```

**Debian / Ubuntu:**
```sh
sudo apt-get install -y unixodbc unixodbc-dev
```

**RHEL / Fedora:**
```sh
sudo dnf install -y unixODBC unixODBC-devel
```

## CCCS Customizations

The following features have been added to dbt-spark beyond the upstream open-source version.

### Iceberg Tblproperties Sync

Iceberg tables support post-creation syncing of `tblproperties`. When a model's `tblproperties` config changes between runs, dbt will detect the diff and issue the appropriate `ALTER TABLE SET TBLPROPERTIES` and `ALTER TABLE UNSET TBLPROPERTIES` statements.

- Properties prefixed with `polaris_` are automatically ignored (never compared or synced).
- Only applies to Iceberg tables (`file_format: iceberg`).
- On new table creation or full refresh, all properties are applied via `SET`.
- On incremental merge runs, a structural diff is computed: new/changed properties are `SET`, removed properties are `UNSET`.

**Usage:**
```yaml
models:
  my_model:
    +file_format: iceberg
    +tblproperties:
      write.format.default: parquet
      history.expire.max-snapshot-age-ms: '86400000'
```

### Column Comment Diff Sync

Column comments are now only updated when they differ from the existing database state. Previously, every incremental run would re-issue `ALTER COLUMN COMMENT` for all columns with descriptions regardless of whether they had changed.

- Uses case-insensitive column name matching.
- Compares the existing column comment (from `DESCRIBE EXTENDED`) against the desired `description` in the model schema.
- Only columns with differing comments trigger an `ALTER` statement.
- Requires `persist_docs.columns: true` in model config.

### Partition Drift Detection

Two behavior flags enable detection of partition spec drift between model config and the existing Iceberg table:

| Flag | Default | Description |
|------|---------|-------------|
| `check_partition_sync` | `false` | Enables partition drift detection on incremental merge runs |
| `check_partition_sync_raises` | `false` | When `true`, raises an error instead of logging a warning |

- Only applies to Iceberg tables.
- Compares full partition transform expressions (e.g., `days(ts)`, `bucket(16, id)`) in order.
- Uses `SHOW CREATE TABLE` to fetch the existing partition spec.
- Runs on the incremental merge path only (new tables and full refreshes are always correct).

**Usage in `dbt_project.yml`:**
```yaml
flags:
  check_partition_sync: true
  check_partition_sync_raises: true  # optional: raise instead of warn
```

**Model config:**
```yaml
models:
  my_model:
    +file_format: iceberg
    +partition_by:
      - days(ts)
      - region
```

If the existing table has a different partition spec (e.g., only `region`), dbt will warn or error with a message showing the mismatch.

### V2 Relation Listing (Default)

By default, dbt-spark now uses `SHOW TABLES` + `DESCRIBE EXTENDED` (v2) as the primary method for listing relations, which is compatible with Iceberg v2 tables. If v2 fails for a non-"not found" reason, it falls back to `SHOW TABLE EXTENDED` (v1) with a debug log.

For legacy Spark catalogs that do not support the v2 path, you can disable v2 relation listing using the `use_v2_relation_listing` behavior flag.

| Flag | Default | Description |
|------|---------|-------------|
| `use_v2_relation_listing` | `true` | Uses `SHOW TABLES` + `DESCRIBE EXTENDED` (v2) first, falling back to v1 on failure. Set to `false` to revert to legacy v1-first listing. |

**Usage in `dbt_project.yml`:**
```yaml
flags:
  use_v2_relation_listing: false  # revert to legacy v1-first listing
```

### Required Location Root (Default)

By default, dbt-spark requires all table materializations to specify a `location_root` config. This ensures tables are always created at explicit, governed storage locations. If `location_root` is missing, dbt raises a compiler error.

To disable this check (e.g., for managed tables where Spark controls the storage location), set the `require_location_root` flag to `false`.

| Flag | Default | Description |
|------|---------|-------------|
| `require_location_root` | `true` | Raises a compiler error when a table materialization does not specify `location_root` |

**Usage in `dbt_project.yml`:**
```yaml
flags:
  require_location_root: false  # allow tables without explicit location
```

### Partition Overwrite Mode SET (Default)

By default, dbt-spark emits `SET spark.sql.sources.partitionOverwriteMode = DYNAMIC` before `insert_overwrite` and `microbatch` operations that use `partition_by`. On Iceberg this is a no-op (Iceberg handles dynamic overwrite natively via its SQL extensions), but it serves as a defensive measure ensuring the session is in the expected state. For non-Iceberg formats (parquet, delta, hive, etc.) this SET is required for correct dynamic partition overwrite behavior.

To disable the SET (e.g., if the session-level statement causes issues in your environment), set `set_partition_overwrite_mode` to `false`.

| Flag | Default | Description |
|------|---------|-------------|
| `set_partition_overwrite_mode` | `true` | Emits `SET spark.sql.sources.partitionOverwriteMode = DYNAMIC` before insert_overwrite/microbatch with partition_by |

**Usage in `dbt_project.yml`:**
```yaml
flags:
  set_partition_overwrite_mode: false  # skip the session SET
```

**Model config:**
```yaml
models:
  my_model:
    +location_root: s3://my-bucket/warehouse
```

### Microbatch Concurrency (Iceberg only)

`dbt-spark` declares `Capability.MicrobatchConcurrency = Full`, allowing dbt-core to
schedule the batches of a `incremental_strategy: microbatch` model in parallel.

**Requirements**

- `file_format: 'iceberg'` is mandatory. Using `microbatch` with any other file
  format (parquet, delta, hudi, hive, ...) raises a compiler error.
- `partition_by` is mandatory (inherited from upstream microbatch validation).
- An Iceberg catalog that supports concurrent commits on disjoint partitions
  (Iceberg snapshot isolation handles the merge automatically).

**Example**

```yaml
models:
  events:
    +incremental_strategy: microbatch
    +file_format: iceberg
    +event_time: event_time
    +batch_size: day
    +begin: '2024-01-01'
    +partition_by:
      - days(event_time)
    # Optional: opt-out of parallel execution.
    # +concurrent_batches: false
```

**How concurrency is made safe**

1. **Per-batch tmp relation suffix.** Each batch's intermediate relation is
   suffixed with `model.batch.id` (e.g. `events__dbt_tmp_20240115`) so
   concurrent batches do not clobber each other's temp objects. Mirrors the
   pattern in `dbt-adapters`'s base `make_temp_relation`.
2. **`partitionOverwriteMode` SET is controlled by the `set_partition_overwrite_mode` flag.**
   The SET is emitted by default (flag defaults to `true`). On Iceberg this is
   a no-op since Iceberg's `INSERT OVERWRITE` uses native dynamic-partition
   semantics via the Iceberg SQL extensions. The flag can be disabled if the
   session-level SET causes issues in your environment.
3. **Per-batch metadata sync is suppressed under concurrency.**
   `check_partition_sync` and `sync_tblproperties` mutate table-level
   metadata and cannot safely run from multiple workers at once. They are
   skipped when dbt-core may schedule batches in parallel and still run when:
   - the execution is not microbatch (regular incremental, full refresh), or
   - the model explicitly sets `+concurrent_batches: false` (dbt-core then
     guarantees a single batch is in flight at a time).

**Limitations**

- Iceberg only. There is no plan to extend this to Delta, Hudi, parquet, or
  Hive in v1.
- When `concurrent_batches` is `true` or unset, partition-spec drift and
  `tblproperties` drift are not reconciled during the microbatch run. Run a
  non-microbatch dbt invocation (or set `+concurrent_batches: false`) to
  perform the reconciliation.
- Spark 3.5.6 cannot submit multi-statement SQL; do not rely on session-level
  `SET` statements to influence the per-batch INSERT.
- First/last batches always run sequentially in dbt-core, but middle batches
  may overlap them; do not assume single-writer semantics for any batch
  unless `+concurrent_batches: false` is set.

### Streaming Materialization (SQL and Python)

The `streaming` materialization starts a Spark Structured Streaming query that
writes to a persistent sink table. It supports SQL and Python models and
requires the local Spark driver submission method:

```yaml
models:
  my_project:
    +submission_method: spark_session_based_cluster
```

dbt assigns each query the target relation name and derives a stable checkpoint
child directory from that target. On a later dbt run, an active query with the
same target name is reused: dbt reconciles supported table metadata but does
not evaluate the model or start another writer.

**Python example**

```python
def model(dbt, spark):
    dbt.config(
        materialized="streaming",
        submission_method="spark_session_based_cluster",
        file_format="iceberg",
        checkpoint_basedir="s3://my-bucket/dbt-checkpoints",
        trigger="30 seconds",
        partition_by=["days(event_time)"],
        tblproperties={"write.format.default": "parquet"},
        write_stream_options={
            "fanout-enabled": "true",
            "snapshot-property.app-id": "my_supervisor_v1",
        },
    )

    return spark.readStream.table("raw.events")
```

**SQL example**

```sql
{{ config(
    materialized="streaming",
    submission_method="spark_session_based_cluster",
    file_format="iceberg",
    checkpoint_basedir="s3://my-bucket/dbt-checkpoints",
    trigger="30 seconds"
) }}

select
    events.event_id,
    events.event_time,
    users.plan
from {{ ref("events") }} as events
join {{ source("raw", "users") }} as users
    on events.user_id = users.user_id
```

For SQL streaming models, dbt resolves every declared `ref()` and `source()` as
a Structured Streaming input with `spark.readStream.table()`, creates an
internal temporary view for it, and runs the compiled SQL against those views.
Every referenced model and source must therefore support Structured Streaming
reads. Batch dimension or lookup joins are not supported by this materialization.

**Streaming model config**

| Config | Default | Description |
|--------|---------|-------------|
| `submission_method` | Required | Must be `spark_session_based_cluster`. |
| `file_format` | `delta` | Sink table file format. Use `iceberg` for `tblproperties` sync and `write_stream_options`. |
| `checkpoint_basedir` | `DBT_STREAMING_CHECKPOINT_BASEDIR`, then `tmp/dbt-streaming-checkpoints` | Parent directory for the target-specific checkpoint. It must be stable and writable by Spark. |
| `trigger` | `DBT_STREAMING_TRIGGER`, then `60 seconds` | Structured Streaming processing-time trigger. |
| `await_termination` | `await_termination` behavior flag | Model-level override for whether dbt waits for the started query. |
| `partition_by` | None | Sink-table partition columns or Iceberg partition transforms. |
| `location_root` | None | Parent location for the sink table, subject to `require_location_root`. |
| `options` | None | Data-source options used when dbt first creates the sink table with DataFrameWriterV2. |
| `tblproperties` | None | Iceberg sink-table properties. Existing Iceberg table properties are reconciled on reruns. |
| `write_stream_options` | None | Iceberg streaming-writer options applied when dbt starts a new query. |
| `read_stream_options` | None | Iceberg streaming-reader options applied to every dbt-managed `ref()` and `source()` input. |

**Wait for termination**

By default, dbt starts the stream and returns without waiting. Set the
`await_termination` behavior flag to make dbt call
`StreamingQuery.awaitTermination()` for streaming models. A model-level
`await_termination` config takes precedence over the flag.

| Flag | Default | Description |
|------|---------|-------------|
| `await_termination` | `false` | Wait for a streaming query to terminate before the dbt run completes. |

```yaml
flags:
  await_termination: true
```

**Local streaming materialization tests**

The opt-in local streaming test starts a local Spark 3.5.6 session and writes a
`rate` stream to a temporary Iceberg catalog. It requires Java 11 or 17 and
Maven/Ivy network access on its first run to fetch the Iceberg runtime package.

```bash
hatch run local-spark-tests:test
```

This test is intentionally separate from the standard unit-test command.

**Iceberg streaming writer options**

`write_stream_options` is valid only when `file_format: iceberg`. Values must be
strings; use the literal strings `"true"` and `"false"` rather than YAML boolean
values. Unsupported option names, non-string values, and non-Iceberg usage raise
a compiler error. This config is supported only by `materialized: streaming`.

| Option | Values | Default | Description |
|--------|--------|---------|-------------|
| `fanout-enabled` | `"true"`, `"false"` | `"false"` | Enables fanout writes. This permits multiple unsorted partition files to be written simultaneously, increasing memory use while avoiding a pre-write shuffle. |
| `check-nullability` | `"true"`, `"false"` | `"true"` | Validates incoming record nulls against the target Iceberg schema. |
| `check-ordering` | `"true"`, `"false"` | `"true"` | Validates incoming batch data against defined target-table sort orders. |
| `snapshot-property.<key>` | String | None | Adds custom metadata to every Iceberg snapshot committed by a streaming micro-batch. |

```yaml
models:
  my_project:
    streaming_events:
      +write_stream_options:
        fanout-enabled: "true"
        check-nullability: "true"
        check-ordering: "false"
        snapshot-property.app-id: my_supervisor_v1
```

`stream_options` has been renamed to `write_stream_options`; update existing
model configuration to use the new name.

**Iceberg streaming reader options**

`read_stream_options` applies the same options to every streaming table read
created by dbt: SQL-model `ref()` and `source()` inputs, and table-valued Python
`dbt.ref()` and `dbt.source()` inputs. It does not modify direct user-authored
`spark.readStream` calls. This config is supported only by `materialized:
streaming`. Option names and values must be strings.

| Option | Accepted alias | Values |
|--------|----------------|--------|
| `stream-from-timestamp` | `streamFromTimestamp` | Epoch milliseconds as a numeric string. |
| `start-snapshot-id` | `startSnapshotId` | Snapshot ID as a numeric string. |
| `end-snapshot-id` | `endSnapshotId` | Snapshot ID as a numeric string. |
| `startingVersion` | None | `"latest"` or a snapshot ID as a numeric string. |
| `split-size` | `splitSize` | Bytes as a numeric string. |
| `streaming-skip-delete-snapshots` | `streamingSkipDeleteSnapshots` | `"true"` or `"false"`. |
| `streaming-skip-overwrite-snapshots` | `streamingSkipOverwriteSnapshots` | `"true"` or `"false"`. |
| `streaming-max-files-per-micro-batch` | `streamingMaxFilesPerMicroBatch` | Numeric string. |
| `streaming-max-rows-per-micro-batch` | `streamingMaxRowsPerMicroBatch` | Numeric string. |

```yaml
models:
  my_project:
    streaming_events:
      +read_stream_options:
        startingVersion: "latest"
        streaming-skip-overwrite-snapshots: "true"
```

**Operational limitations**

- Only `spark_session_based_cluster` is supported; job-cluster and remote
  submission methods cannot manage persistent local streaming queries.
- dbt does not stop or restart an active stream. Updated `write_stream_options`
  and `read_stream_options` take effect only after the current query is stopped
  and dbt starts a new one.
- SQL model relation references are rewritten with SQLGlot, preserving aliases,
  CTEs, string literals, and comments while substituting the internal streaming
  temporary views.

## Contribute

- Want to help us build `dbt-spark`? Check out the [Contributing Guide](CONTRIBUTING.md).
