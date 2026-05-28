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

## Additional Configuration for MacOS

If installing on MacOS, use `homebrew` to install required dependencies.
   ```sh
   brew install unixodbc
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

## Contribute

- Want to help us build `dbt-spark`? Check out the [Contributing Guide](CONTRIBUTING.md).
