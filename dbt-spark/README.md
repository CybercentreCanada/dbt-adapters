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

### Session-Based Python Models

Python models can execute in the dbt process using
`submission_method: spark_session_based_cluster`. Configure the profile with
`method: session` as well; the connection method alone does not select the
Python submission helper. Existing-target incremental runs use session-scoped
temporary views instead of physical staging tables.

See the [session Python execution specification](specs/spec-design-spark-session-python-model-execution.md)
for configuration, execution and conversion rules, session-sharing assumptions,
dependency requirements, and limitations including timeout, cleanup, and
replacement behavior.

### SQL and Python Scalar Functions with Polaris

This fork supports **session-scoped** dbt function resources on Apache Spark
**4.1.3** with an Apache Polaris Iceberg REST catalog. SQL functions use Spark's
native `CREATE OR REPLACE TEMPORARY FUNCTION`; Python functions use
`spark.udf.register`. They execute in Spark, **not** in Polaris, and are removed
when the dbt invocation finishes.

Install this adapter and a function-capable dbt-core runtime:

```sh
pip install -e '.[session]' 'pyspark==4.1.3' 'dbt-core>=1.11,<2'
```

The tested combination is Spark 4.1.3, Iceberg 1.12.0, and Polaris 1.8.0.
The native dbt integration was verified with dbt-core 1.14.0a1; the minimum
function-capable dbt-core release is not independently certified here.
Provision compatible Python interpreters and imported libraries on the driver
and executors. `runtime_version` validates the driver; it does not install an
interpreter or dependencies.

#### Configure the Spark session and REST catalog

For example, in your dbt profile:

```yaml
spark_polaris:
  target: dev
  outputs:
    dev:
      type: spark
      method: session
      host: localhost
      schema: analytics
      threads: 4
      server_side_parameters:
        spark.master: "local[2]"
        spark.jars.packages: "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.12.0,org.apache.iceberg:iceberg-aws-bundle:1.12.0"
        spark.sql.catalog.polaris: org.apache.iceberg.spark.SparkCatalog
        spark.sql.catalog.polaris.type: rest
        spark.sql.catalog.polaris.uri: "{{ env_var('POLARIS_CATALOG_URI') }}"
        spark.sql.catalog.polaris.warehouse: "{{ env_var('POLARIS_CATALOG_NAME') }}"
        spark.sql.catalog.polaris.rest.auth.type: oauth2
        spark.sql.catalog.polaris.oauth2-server-uri: "{{ env_var('POLARIS_TOKEN_URI') }}"
        spark.sql.catalog.polaris.credential: "{{ env_var('POLARIS_CLIENT_CREDENTIAL') }}"
        spark.sql.catalog.polaris.scope: "PRINCIPAL_ROLE:ALL"
        spark.sql.catalog.polaris.io-impl: org.apache.iceberg.aws.s3.S3FileIO
        spark.sql.defaultCatalog: polaris
```

`POLARIS_CATALOG_URI` is the catalog API base URI, commonly
`https://<host>/api/catalog`; `POLARIS_TOKEN_URI` is the complete OAuth token
endpoint, commonly `https://<host>/api/catalog/v1/oauth/tokens`.
`POLARIS_CLIENT_CREDENTIAL` has the `client_id:client_secret` format.
`warehouse` is the Polaris catalog name, not an S3 path. Grant the principal
access to that catalog and configure storage credentials/credential vending as
required by your deployment. REST authentication alone does not grant Spark
access to S3. The example uses S3; other storage requires its compatible
Iceberg FileIO integration. Keep secrets in environment variables, not source
control.

**Parser limitation:** omit `IcebergSparkSessionExtensions` from
`spark.sql.extensions` for SQL UDFs with the tested Iceberg 1.12.0 runtime.
That extension throws `UnsupportedOperationException` in `parseRoutineParam`.
The adapter reports this configuration error; it does not silently remove the
extension. Use a fresh Spark process after changing static extension settings.
Extension-specific Iceberg statements are not supported in this configuration.

Without the Iceberg extension, this runtime also lacks persistent view
discovery. Explicitly opt into **table-only** metadata discovery in
`dbt_project.yml`:

```yaml
profile: spark_polaris
flags:
  spark_table_only: true
models:
  +database: polaris
  +file_format: iceberg
  +submission_method: spark_session_based_cluster
  +location_root: "{{ env_var('DBT_MODEL_LOCATION_ROOT') }}"
functions:
  +spark_function_scope: session
```

`spark_table_only` defaults to `false`. When enabled it omits persistent views
from discovery/catalog output, rejects `view` and `materialized_view`
materializations, and uses table-only v2 discovery even if
`use_v2_relation_listing` is disabled. It does not discover or manage existing
views; use a namespace reserved for dbt tables, without conflicting view names.
Other profiles retain normal view discovery. This flag does not affect temporary
views used internally for Python incremental staging.

The catalog belongs in the model `database` configuration, not the profile
`database` field. `DBT_MODEL_LOCATION_ROOT` must be a writable storage parent
inside the Polaris catalog's allowed locations. Function registration has no
storage location.

#### Define functions and build them with their consumers

Create SQL function source under `functions/double_amount.sql`:

```sql
amount * 2
```

Create Python function source under `functions/multiply_amount.py`:

```python
if amount is None or multiplier is None:
    return None
return amount * multiplier
```

Declare properties in `functions/schema.yml` (the example driver uses Python
3.12; change the runtime declaration to match your interpreter):

```yaml
functions:
  - name: double_amount
    arguments:
      - name: amount
        data_type: INT
    returns:
      data_type: INT
  - name: multiply_amount
    config:
      runtime_version: "3.12"
      entry_point: multiply_amount_impl
    arguments:
      - name: amount
        data_type: INT
      - name: multiplier
        data_type: INT
    returns:
      data_type: INT
```

Python source is a **function body**, not `model(dbt, spark)` and not a complete
named handler. `entry_point` names the generated wrapper. Arguments preserve
their declared order; the Spark return type is explicit. Python argument type
metadata does not enforce a warehouse-style input signature.

Create `models/amount_report.sql`:

```sql
{{ config(materialized='table') }}
select {{ function('double_amount') }}(3) as doubled,
       {{ function('multiply_amount') }}(3, 4) as multiplied
```

Build the functions and consumers in **one invocation**:

```sh
dbt build --select +amount_report
```

`function()` creates native dbt DAG dependencies and renders an unqualified,
quoted name. The adapter replays successfully built definitions into each actual
consuming Spark session. Functions with the same alias in different dbt
schemas/packages still collide because temporary functions are not namespaced.
Existing Spark/user functions are not overwritten.

Session Python models may use these registered names through Spark SQL or
`selectExpr`. The current dbt Python API has **no `dbt.function()` method**.
Establish ordering through `dbt.ref()` to a SQL model that references the UDFs:

```python
def model(dbt, spark):
    dbt.config(materialized="table")
    upstream = dbt.ref("amount_report")
    return upstream.selectExpr("`double_amount`(3) as doubled")
```

Build that Python consumer with its ancestors as well.
Literal Spark function names alone do not create dbt dependencies.

**Limitations:** no persistent Polaris UDF deployment, cross-engine access,
function grants, persistent function documentation, overloads, argument
defaults, aggregate/table/JavaScript/vectorized functions, remote Python
submission, or streaming consumers. Persistent views referencing temporary
functions are not supported. SQL/Python nondeterminism is supported;
`volatility: stable` is rejected. Python code is trusted executable project
code, not a sandbox.

A function-only build does not deploy anything for a later process. A subsequent
model-only build or deferred function reference must rebuild required functions;
`--defer` is not a deployment mechanism for session functions.

See the [function registration specification](specs/spec-design-spark-sql-python-udf-registration.md)
for interfaces, lifecycle, and tests.

#### Validate function support

From this adapter directory:

```sh
DBT_RUN_SPARK_FUNCTION_TESTS=1 hatch run local-spark-iceberg:python -m pytest \
  --noconftest tests/unit/test_functions.py tests/unit/test_functions_live.py -n 0
hatch run local-spark-iceberg:python -m pytest \
  tests/functional/adapter/test_functions.py --profile spark_session -n 0
```

The real Polaris test uses a separately provisioned Polaris service/catalog:
set `DBT_SPARK_POLARIS_URI`, `DBT_SPARK_POLARIS_CATALOG`,
`DBT_SPARK_POLARIS_CREDENTIAL`, and, when needed,
`DBT_SPARK_POLARIS_TOKEN_URI` and `DBT_SPARK_POLARIS_SCOPE`, then run:

```sh
hatch run local-spark-iceberg:python -m pytest \
  tests/functional/adapter/test_functions.py --profile polaris -n 0
```

For local S3-compatible test storage, additionally set
`DBT_SPARK_POLARIS_S3_ENDPOINT`, `DBT_SPARK_POLARIS_S3_ACCESS_KEY`, and
`DBT_SPARK_POLARIS_S3_SECRET_KEY`. These are test-only inputs, never committed
credentials. Without the Polaris URI/catalog variables, that integration test
is skipped rather than treated as a pass.

### Fast Iceberg Relation Listing

<!-- CCCS: session-only catalog detection and constant-query relation discovery. -->
With a `session` connection, catalogs implemented by
`org.apache.iceberg.spark.SparkCatalog` use `SHOW TABLES` and `SHOW VIEWS` to
discover persistent tables and views. This takes two listing queries regardless
of table count, with no per-table `DESCRIBE`. Temporary tables/views are excluded,
including temporary views that share a persistent table's name.

Both `iceberg_listing_fast` and `use_v2_relation_listing` default to `true`.
To use v2-first listing with per-relation metadata lookup, set
the following in `dbt_project.yml`:

```yaml
flags:
  iceberg_listing_fast: false
```

Setting `use_v2_relation_listing: false` instead retains the existing v1-first
listing, regardless of `iceberg_listing_fast`. `ICEBERG_LISTING_FAST` is not an
environment-variable switch.

Catalog detection uses the same configured Spark session as query execution.
Confirmed classifications (including non-Iceberg catalogs) are cached per adapter,
Spark session, and catalog; concurrent namespace listings share the cached probe.
Absent catalog configuration uses a JVM catalog-class lookup. Unknown results
are debug-logged, are not cached, and retain regular listing. A replacement
session is checked independently; catalog registrations are assumed stable
within a session. Disable the flag if changing catalog implementations in place.

Remote connections, custom catalog classes, and the mixed-provider
`SparkSessionCatalog` retain regular listing. Fast-listed relations have empty
`information` strings; columns are fetched separately when needed. Tables are
marked Iceberg, while views are not. Missing namespaces return an empty list;
other listing errors propagate, including lack of `SHOW VIEWS` support.
DESCRIBE-based listing also merges persistent views from `SHOW VIEWS` for
Iceberg session catalogs, including the v1-to-v2 fallback. These catalogs
therefore require a runtime supporting view listing with either listing mode.

Regular listing renders DESCRIBE targets with relation include/quote policies.
An individual table or view that disappears during discovery is skipped without
discarding the surviving relations. Permission and runtime errors propagate;
only unsupported listing operations trigger the legacy listing fallback.
Catalog column entries preserve the relation's catalog identity and occur once
per discovered column, including tables and views returned by fast listing.

The focused live test requires a configured Iceberg session with persistent-view
support. In that environment, run:

```sh
DBT_RUN_ICEBERG_LISTING_TESTS=1 hatch run python -m pytest \
  tests/functional/adapter/test_iceberg_listing.py --profile spark_session -n 0
```

### Local Spark Iceberg Functional Tests

Run all local test stages with the [local test runner](scripts/run_local_tests.sh):

```sh
bash scripts/run_local_tests.sh -q
```

The runner works from any directory, defaults `SPARK_LOCAL_IP` to loopback,
starts the Compose catalog, waits for REST readiness, and stops it on exit
(including failures and interrupts). It refuses existing Compose containers or
an already-responsive catalog so it cannot take over another test session.
Docker daemon access and the prerequisites below are required.

It runs `local-spark-iceberg:adapter-tests`, `local-spark-iceberg:functional-tests`,
`local-spark-iceberg:unit-tests`, and `local-spark-tests:test` sequentially.
Adapter tests are intentionally repeated within the full functional stage.
Pytest arguments are forwarded to every stage; each stage reports pass/fail,
remaining stages still run after test failures, and any test or cleanup failure
produces a nonzero exit code. Warehouse files are retained.

The local unit target selects column, comment, partition, property, local-profile,
session correctness, and runner tests without loading transport fixtures. It does
**not** replace the full `hatch run unit-tests` suite or write-order certification.

The opt-in local functional target runs the Iceberg tblproperties, microbatch,
relation-listing, Python incremental staging-location, adapter, persisted-comment,
partition-drift, and SQL contract tests against an in-process Spark 4.1.3 session
and Iceberg 1.12.0. It uses a dedicated `local_spark_iceberg` profile with an
Iceberg `SparkCatalog` connected to a local REST catalog, backed by a warehouse under
`.hatch/local-spark-iceberg/warehouse`; this profile is separate from the
existing Spark and Databricks profiles. The target does not run the
Databricks-only Python incremental model location test.

Install Java 17, Hatch, Docker with Compose, and curl. Ensure Maven/Ivy can access
Maven Central to resolve Iceberg's Spark runtime package on the first run, and
Docker can pull the catalog image. Then run from
`dbt-spark/`:

```sh
mkdir -p .hatch/local-spark-iceberg/warehouse
export LOCAL_UID="$(id -u)" LOCAL_GID="$(id -g)"
docker compose -f docker/compose.local-spark-iceberg.yml up -d
curl --fail --retry 20 --retry-connrefused --retry-delay 1 http://127.0.0.1:8181/v1/config
hatch run local-spark-iceberg:functional-tests
docker compose -f docker/compose.local-spark-iceberg.yml down
```

This command creates a dedicated Hatch environment with the functional test
dependencies, editable adapter packages, and PySpark 4.1.3, and enables the
Iceberg listing test's opt-in flag. The independent environment uses a
self-referencing Hatch template, not detached mode (which disables project
installation).

The loopback-only REST service uses a pinned Apache reference-server image
(Iceberg server 1.10.1), with a JDBC backend and persistent-view support. Spark
still uses `org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.12.0`.
A HadoopCatalog cannot run the persistent-view listing test. Host and container
mount the warehouse at the same absolute path; run Compose and Hatch from the
same `dbt-spark` directory on the same host. The catalog database is disposable,
while warehouse files remain in the dedicated ignored directory.

Only this profile sets model catalog/file-format defaults to Iceberg and
disables `require_location_root` so catalog-managed locations are tested.
It explicitly drops test relations before namespace teardown because REST
catalogs do not implement Spark's cascading namespace deletion. Default
profiles and CI jobs are unchanged. Run `docker compose ... down` even if pytest
fails.

The Python staging tests run a model twice through dbt with merge, checking
exact updated/inserted rows and catalog-reported storage locations. A scoped,
test-only submission helper executes the real generated Python locally under a
distinct helper name to exercise physical staging. It verifies
`location_root/orders__dbt_tmp` differs from the permanent
`location_root/orders` and that the staging table is removed. A second case
uses `spark_session_based_cluster` to verify temporary-view staging remains
unchanged (the view remains session-scoped until test teardown). Unique test
storage roots are cleaned after their relations. These tests do not certify
Databricks transport, custom staging hooks, or physical microbatch execution.

To run only the live adapter tests with `type: spark` and `method: session`,
start the REST catalog as above, then run:

```sh
SPARK_LOCAL_IP=127.0.0.1 hatch run local-spark-iceberg:adapter-tests
```

These tests exercise actual session connections and configured settings, query
results and error propagation/recovery, column metadata, and persistent
table/view listing with temporary relations excluded, plus the portable feature
coverage below. They are also included in `local-spark-iceberg:functional-tests`.
They do not mock Spark or require HTTP/Thrift/ODBC adapter credentials. The
live counterparts complement the unit suite; they are integration tests, not
unit tests.

| Existing coverage | Local session counterpart |
| --- | --- |
| Column comment and metadata-sync units | [Persisted comments](tests/functional/adapter/test_metadata_session.py): create, update, clear, case-insensitive column matching, escaped descriptions, and unchanged-doc diffs on real metadata |
| Partition-sync units | [Partition policies](tests/functional/adapter/test_metadata_session.py): plain/transform partitions, warning/error/disabled modes, unchanged partition specs, and writes blocked on errors |
| Tblproperties units | [Property sync](tests/functional/adapter/tblproperties/test_iceberg_tblproperties.py): create, project override, add/update/remove, and preservation of `polaris_` catalog-managed properties |
| Non-session SQL constraints tests and column types | [SQL contracts](tests/functional/adapter/test_constraints_session.py): table/incremental/view schema validation, seven Spark data types, table column ordering, exact row values, and rejected name/type mismatches without publishing a relation |

SQL schema contracts are not row-level constraint enforcement. The local target
does not add Delta or certify its CHECK/NOT NULL enforcement tests. Python model
contracts remain excluded because dbt-core rejects them; see the
[Python contracts ADR](docs/arch/adr-0002-spark-python-model-contracts.md).
Transport polling/retry tests, credential validation, telemetry, and pure
Python parsing tests remain in the unit suite. Streaming/batch writer tests
remain in `local-spark-tests:test`; write-order certification retains its
separate Spark 4.0 environment.

### Session cursor and partition correctness

Session cursors discard the previous result before each execution attempt,
including failed attempts. `fetchone()` and `fetchall()` consume the same
ordered result stream; exhausted results return `None` and `[]`, respectively.
Rows are read using Spark's local iterator instead of collecting the entire
result for `fetchone()`. Closing a cursor releases its result references, not
the shared Spark session. Cursor context managers propagate body exceptions.

When `check_partition_sync` is enabled for Iceberg, configured and discovered
partition expressions share quote-aware normalization. Quoting regular column
names or changing insignificant transform whitespace does not cause drift;
quoted punctuation, escaped backticks, transform arguments, and partition order
remain significant. Session checks use the active `spark.sql.caseSensitive`
setting. Invalid configuration or unparseable/empty SHOW CREATE TABLE metadata
raises an error even in drift-warning mode. Valid unpartitioned DDL still matches
omitted partition configuration. Genuine drift retains the existing
`check_partition_sync_raises` warning/error policy.

These fixes cover session cursor, relation/catalog metadata, and partition
comparison only. They do not address or certify Databricks command helpers,
non-session query replay/retries, or PySpark-free non-session imports.

Iceberg Python writes pass the configured location as a DataFrameWriterV2
table property; Iceberg ignores the corresponding writer option. This applies
to permanent/staging tables and empty tables initialized for streaming.
New Iceberg Python tables with `location_root` now honor that path instead of
silently using a catalog-managed location. Existing tables are not relocated,
and non-Iceberg writers retain the location option.

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

### Python Partition Transform Compatibility

Python table and incremental models keep the same `partition_by` syntax, for example
`partition_by=["hours(ingest_loaded_by)", "region"]`. When creating a physical table,
the generated Python uses `pyspark.sql.functions.partitioning` when available
(Spark 4.0+) and otherwise uses the legacy functions namespace. This avoids Spark 4
deprecation warnings for `years`, `months`, `days`, `hours`, and `bucket` without
changing the table's partition transforms. Do not prefix transforms in model config
with `partitioning.`.

This compatibility handling applies to batch Python table creation, including
physical incremental staging tables, and to sink creation for both Python and SQL
streaming models. Temporary-view staging does not apply partition transforms.
SQL partition syntax and microbatch processing windows are unchanged.

### Iceberg Write Order

Batch SQL and Python `table` and `incremental` models can configure Iceberg write
ordering on **Spark 4.0.x** and **Iceberg table formats v2/v3**:

```sql
{{ config(
    materialized='incremental',
    file_format='iceberg',
    write_order=['customer_id ASC NULLS LAST', 'total DESC']
) }}

select customer_id, total from {{ ref('source_orders') }}
```

```python
def model(dbt, spark):
    dbt.config(
        materialized="table",
        file_format="iceberg",
        submission_method="spark_session_based_cluster",
        write_order=["customer_id ASC NULLS LAST", "total DESC"],
    )
    return dbt.ref("source_orders")
```

`write_order` must be a list of column references with optional direction and null
placement. Backtick-quoted and nested references are supported. Direction defaults
to `ASC`; null placement defaults to `NULLS FIRST` for ascending terms and
`NULLS LAST` for descending terms. Equivalent configurations do not cause an ALTER.
Column sequence is significant. Scalar strings, nulls, blank entries, duplicate
references, transforms, and arbitrary SQL expressions are rejected.

On supported targets, **omitting `write_order` or setting it to `[]` means
`WRITE UNORDERED`**, including clearing an order from an earlier run. Changed
policies are synchronized using `ALTER TABLE ... WRITE ORDERED BY ...` or
`ALTER TABLE ... WRITE UNORDERED`. No historical data rewrite is initiated, and
write order does not guarantee SELECT result ordering.

- New targets are schema-created, configured, then populated. Validation uses an
  isolated empty probe table before target mutation. Failed initial configuration
  or insertion removes only a newly created table owned by that operation.
  This path does not guarantee atomic publication to concurrent readers.
- Existing incremental targets synchronize before target data writes; internal
  staging relations do not inherit the target order. Microbatch uses the existing
  once-per-target metadata synchronization guard; a failed first synchronization
  prevents later batches from writing.
- With `on_schema_change='append_new_columns'` or `'sync_all_columns'`, new sort
  columns are validated against an isolated staging-schema probe before target
  mutation. Target schema changes precede order synchronization and data writes.
  Invalid references leave target schema and metadata unchanged. Existing
  restrictions on dropping Iceberg columns remain unchanged.
- Existing table replacements and full refreshes remain atomic. **Their initial
  replacement write is an exception: the desired policy is applied afterward.**
  A failed replacement write preserves the old table. If replacement succeeds
  but the subsequent ordering ALTER fails, dbt fails explicitly and leaves the
  replacement in place without attempting rollback.
- Current order is captured from DESCRIBE metadata and resolved using authoritative
  Iceberg metadata when needed. Incomplete or ambiguous metadata is never silently
  treated as unordered.

Prerequisites include preconfigured Iceberg SQL extensions, a matching runtime
artifact, catalog CREATE/DROP permissions for isolated probe tables, and Spark
read access to the catalog's metadata JSON files. Atomic replacement requires
`org.apache.iceberg.spark.SparkCatalog`, not `SparkSessionCatalog`. The tested
baseline is Spark **4.0.3**, Scala **2.13**, Iceberg library **1.10.0**
(`org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0`), with a Hadoop catalog.
Other catalogs and Spark 4.x minor lines require separate verification.

Iceberg table format versions are not library versions. Existing
`tblproperties={'format-version': '3'}` configuration can select v3 for creation;
this feature does not automatically upgrade tables or change format defaults.
Explicit `write_order`, including `[]`, is rejected on unsupported runtimes/formats,
non-Iceberg models, views, ephemeral models, and streaming materializations.
Omission on unsupported surfaces preserves their existing behavior.

Run the dedicated live matrix with Java 17 and:

```shell
cd dbt-spark
hatch run write-order-tests:test
```

`DBT_ICEBERG_JAR` may point to a preinstalled matching runtime JAR instead of
resolving the artifact through Maven. See the
[write-order specification](specs/spec-design-iceberg-write-order.md) for the full
configuration and failure contracts.

### Behavior Flag Specifications

The adapter registers the following project-level flags. These specifications
describe their defaults, scope, interactions, and validation criteria.

| Flag | Default | Specification |
|------|---------|---------------|
| `check_partition_sync` | `false` | [Partition drift detection](../spec/spec-design-spark-flag-check-partition-sync.md) |
| `check_partition_sync_raises` | `false` | [Partition drift severity](../spec/spec-design-spark-flag-check-partition-sync-raises.md) |
| `use_v2_relation_listing` | `true` | [Relation listing preference](../spec/spec-design-spark-flag-use-v2-relation-listing.md) |
| `iceberg_listing_fast` | `true` | [Iceberg fast listing](../spec/spec-design-spark-flag-iceberg-listing-fast.md) |
| `set_partition_overwrite_mode` | `true` | [Partition overwrite setting](../spec/spec-design-spark-flag-set-partition-overwrite-mode.md) |
| `require_location_root` | `true` | [Explicit storage locations](../spec/spec-design-spark-flag-require-location-root.md) |
| `await_termination` | `false` | [Streaming model execution](specs/spec-design-spark-streaming-models.md) |

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
| `file_format` | `iceberg` | Sink table format. `tblproperties` sync and `write_stream_options` require Iceberg. |
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

**Local streaming and batch materialization tests**

The opt-in local tests start Spark 3.5.6 sessions, write a `rate` stream to a
temporary Iceberg catalog, and verify batch partition transforms against stored
Iceberg metadata. They require Java 11 or 17 and Maven/Ivy network access on their
first run to fetch the Iceberg runtime package.

```bash
hatch run local-spark-tests:test
```

These tests are intentionally separate from the standard unit-test command.

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
