{% materialization table, adapter = 'spark', supported_languages=['sql', 'python'] %}
  {%- set language = model['language'] -%}
  {%- set identifier = model['alias'] -%}
  {%- set grant_config = config.get('grants') -%}

  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database,
                                                type='table') -%}

  {{ run_hooks(pre_hooks) }}

  -- setup: if the target relation already exists, drop it
  -- in case if the existing and future table is delta or iceberg, we want to do a
  -- create or replace table instead of dropping, so we don't have the table unavailable
  {% if old_relation is not none %}
    {% set is_delta = (old_relation.is_delta and config.get('file_format', validator=validation.any[basestring]) == 'delta') %}
    {% set is_iceberg = (old_relation.is_iceberg and config.get('file_format', validator=validation.any[basestring]) == 'iceberg') %}
    {% set old_relation_type = old_relation.type %}
  {% else %}
    {% set is_delta = false %}
    {% set is_iceberg = false %}
    {% set old_relation_type = target_relation.type %}
  {% endif %}

  {% if not is_delta and not is_iceberg %}
    {% set existing_relation = target_relation %}
    {{ adapter.drop_relation(existing_relation.incorporate(type=old_relation_type)) }}
  {% endif %}

  -- build model
  {%- call statement('main', language=language) -%}
    {{ create_table_as(False, target_relation, compiled_code, language) }}
  {%- endcall -%}

  {% set should_revoke = should_revoke(old_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {% do persist_constraints(target_relation, model) %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]})}}

{% endmaterialization %}


{% macro py_write_table(compiled_code, target_relation) %}
{{ compiled_code }}
# --- Autogenerated dbt materialization code. --- #
dbt = dbtObj(spark.table)
df = model(dbt, spark)

# make sure pyspark exists in the namepace, for 7.3.x-scala2.12 it does not exist
import pyspark
# make sure pandas exists before using it
try:
  import pandas
  pandas_available = True
except ImportError:
  pandas_available = False

# make sure pyspark.pandas exists before using it
try:
  import pyspark.pandas
  pyspark_pandas_api_available = True
except ImportError:
  pyspark_pandas_api_available = False

# make sure databricks.koalas exists before using it
try:
  import databricks.koalas
  koalas_available = True
except ImportError:
  koalas_available = False

# preferentially convert pandas DataFrames to pandas-on-Spark or Koalas DataFrames first
# since they know how to convert pandas DataFrames better than `spark.createDataFrame(df)`
# and converting from pandas-on-Spark to Spark DataFrame has no overhead
if pyspark_pandas_api_available and pandas_available and isinstance(df, pandas.core.frame.DataFrame):
  df = pyspark.pandas.frame.DataFrame(df)
elif koalas_available and pandas_available and isinstance(df, pandas.core.frame.DataFrame):
  df = databricks.koalas.frame.DataFrame(df)

# convert to pyspark.sql.dataframe.DataFrame
if isinstance(df, pyspark.sql.dataframe.DataFrame):
  pass  # since it is already a Spark DataFrame
elif pyspark_pandas_api_available and isinstance(df, pyspark.pandas.frame.DataFrame):
  df = df.to_spark()
elif koalas_available and isinstance(df, databricks.koalas.frame.DataFrame):
  df = df.to_spark()
elif pandas_available and isinstance(df, pandas.core.frame.DataFrame):
  df = spark.createDataFrame(df)
else:
  msg = f"{type(df)} is not a supported type for dbt Python materialization"
  raise Exception(msg)

writer = df.write.mode("overwrite").format("{{ config.get('file_format', 'delta') }}").option("overwriteSchema", "true")
{% if partition_cols() -%}
writer = writer.partitionBy('{{ partition_cols() | trim }}'.strip("()").split(","))
{%- endif %}
{% if location_clause() -%}
writer = writer.option("path", "{{ location_clause() | trim }}".split("'")[1])
{%- endif %}
writer.saveAsTable("{{ target_relation }}")
{%- endmacro -%}

{%macro py_script_comment()%}
# how to execute python model in notebook
# dbt = dbtObj(spark.table)
# df = model(dbt, spark)
{%endmacro%}
