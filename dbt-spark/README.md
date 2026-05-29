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

For legacy Spark catalogs that do not support the v2 path, you can revert to the original v1-first behavior using the `use_v1_relation_listing` behavior flag.

| Flag | Default | Description |
|------|---------|-------------|
| `use_v1_relation_listing` | `false` | Uses `SHOW TABLE EXTENDED` (v1) first, falling back to v2 on the "not supported for v2 tables" error |

**Usage in `dbt_project.yml`:**
```yaml
flags:
  use_v1_relation_listing: true  # revert to legacy v1-first listing
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
2. **No `partitionOverwriteMode` SET on the Iceberg path.** Iceberg's
   `INSERT OVERWRITE` uses native dynamic-partition semantics via the Iceberg
   SQL extensions and ignores `spark.sql.sources.partitionOverwriteMode`. The
   session-level SET is therefore skipped, which also removes a race that
   would otherwise be unavoidable: Apache Spark 3.5.6 cannot submit
   multi-statement SQL, so the SET cannot be co-located with the INSERT in a
   single submission.
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

## Contribute

- Want to help us build `dbt-spark`? Check out the [Contributing Guide](CONTRIBUTING.md).
