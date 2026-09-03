{% materialization streaming, adapter='spark', supported_languages=['sql', 'python'] %}
  {%- set language = model['language'] -%}
  {%- set identifier = model['alias'] -%}
  {%- set grant_config = config.get('grants') -%}
  {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database,
                                                type='table') -%}
  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}

  {%- if config.get('submission_method') != 'spark_session_based_cluster' -%}
    {{ exceptions.raise_compiler_error("The Spark streaming materialization requires submission_method='spark_session_based_cluster'.") }}
  {%- endif -%}

  {{ run_hooks(pre_hooks) }}

  {%- if old_relation is not none -%}
    {% do adapter.check_partition_sync(target_relation, config.get('file_format'), config.get('partition_by')) %}
    {% do sync_tblproperties(target_relation, config.get('tblproperties')) %}
  {%- endif -%}

    {%- set execution_language = 'python' if language == 'sql' else language -%}
    {%- call statement('main', language=execution_language) -%}
        {% if language == 'sql' %}
            {{ sql_stream_table(compiled_code, target_relation, old_relation is not none) }}
        {% else %}
            {{ py_stream_table(compiled_code, target_relation, old_relation is not none) }}
        {% endif %}
  {%- endcall -%}

  {% set should_revoke = should_revoke(old_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke) %}
  {% do persist_docs(target_relation, model) %}
  {% do persist_constraints(target_relation, model) %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}


{% macro py_stream_table(compiled_code, target_relation, target_exists) %}
import os
import pyspark

target_name = "{{ target_relation }}"
active_query = next((query for query in spark.streams.active if query.name == target_name), None)

if active_query is None:
{{ compiled_code | indent(4, true) }}

    def load_df_from_table_or_sql(source_or_ref_value):
        if (source_or_ref_value.strip().startswith("(") and source_or_ref_value.strip().endswith(")")) or "select " in source_or_ref_value.lower():
            return spark.sql(source_or_ref_value)
        reader = spark.readStream
    {{ spark__read_stream_options_clause('reader') | indent(4, true) }}
        return reader.table(source_or_ref_value)

    dbt = dbtObj(load_df_from_table_or_sql)
    df = model(dbt, spark)

{{ start_stream('df', target_exists) | indent(4, true) }}
else:
    print(f"Stream {target_name} is already active (id={active_query.id}); skipping startup. Changed write_stream_options and read_stream_options take effect after the stream is restarted.")

{% set await_termination = config.get('await_termination') %}
{% if await_termination is none %}
{% set await_termination = adapter.behavior.await_termination.no_warn %}
{% endif %}
if {{ await_termination }}:
    active_query.awaitTermination()
{% endmacro %}


{% macro sql_stream_table(compiled_code, target_relation, target_exists) %}
import pyspark
from dbt.adapters.spark.sql_rewrite import rewrite_streaming_sql

target_name = "{{ target_relation }}"
active_query = next((query for query in spark.streams.active if query.name == target_name), None)

if active_query is None:
    stream_inputs = {
        {%- set input_index = namespace(value=0) -%}
        {%- for ref_node in model.refs -%}
            {%- set ref_args = [ref_node.get('package'), ref_node['name']] if ref_node.get('package') else [ref_node['name']] -%}
            {%- set resolved = ref(*ref_args, v=ref_node.get('version')) -%}
            {%- if resolved.render is defined and resolved.render is callable -%}
                {%- set resolved = resolved.render() -%}
            {%- endif -%}
            "{{ resolved }}": "__dbt_stream_input_{{ input_index.value }}",
            {%- set input_index.value = input_index.value + 1 -%}
        {%- endfor -%}
        {%- for source_node in model.sources -%}
            {%- set resolved = source(*source_node) -%}
            {%- if resolved.render is defined and resolved.render is callable -%}
                {%- set resolved = resolved.render() -%}
            {%- endif -%}
            "{{ resolved }}": "__dbt_stream_input_{{ input_index.value }}",
            {%- set input_index.value = input_index.value + 1 -%}
        {%- endfor -%}
    }
    for relation, temporary_view in stream_inputs.items():
        reader = spark.readStream
    {{ spark__read_stream_options_clause('reader') | indent(4, true) }}
        reader.table(relation).createOrReplaceTempView(temporary_view)

    df = spark.sql(rewrite_streaming_sql({{ compiled_code | tojson }}, stream_inputs))
{{ start_stream('df', target_exists) | indent(4, true) }}
else:
    print(f"Stream {target_name} is already active (id={active_query.id}); skipping startup. Changed write_stream_options and read_stream_options take effect after the stream is restarted.")

{% set await_termination = config.get('await_termination') %}
{% if await_termination is none %}
{% set await_termination = adapter.behavior.await_termination.no_warn %}
{% endif %}
if {{ await_termination }}:
    active_query.awaitTermination()
{% endmacro %}


{% macro start_stream(dataframe, target_exists) %}
if not isinstance({{ dataframe }}, pyspark.sql.dataframe.DataFrame):
    raise TypeError(f"{type({{ dataframe }})} is not a supported type for dbt streaming materialization")
if not {{ dataframe }}.isStreaming:
    raise TypeError("dbt streaming models must produce a streaming PySpark DataFrame")

if not {{ target_exists }}:
    from pyspark.sql.functions import years, months, days, hours, bucket

    writer = spark.createDataFrame([], {{ dataframe }}.schema).writeTo(target_name).using("{{ config.get('file_format', 'delta') }}")
{{ python__partitionedBy_clause() | indent(2, true) }}
{% for option, value in (config.get('options') or {}).items() -%}
    writer = writer.option("{{ option }}", "{{ spark__escape_single_quotes(value) }}")
{% endfor -%}
{{ python__location_clause() | trim | indent(4, true) }}
{{ python__tblproperties_clause() | indent(2, true) }}
    writer.create()

checkpoint_basedir = "{{ config.get('checkpoint_basedir') or env_var('DBT_STREAMING_CHECKPOINT_BASEDIR', 'tmp/dbt-streaming-checkpoints') }}"
if not checkpoint_basedir or checkpoint_basedir == "None":
    raise ValueError("checkpoint_basedir must be configured")
checkpoint_location = f"{checkpoint_basedir}/{target_name}"
trigger = "{{ config.get('trigger') or env_var('DBT_STREAMING_TRIGGER', '60 seconds') }}"

active_query = (
    {{ dataframe }}.writeStream
    .queryName(target_name)
    .option("checkpointLocation", checkpoint_location)
{{ spark__write_stream_options_clause() | indent(4, true) }}
    .trigger(processingTime=trigger)
    .toTable(target_name)
)
print(f"Started stream {target_name} (id={active_query.id}, checkpoint={checkpoint_location})")
{% endmacro %}


{% macro spark__write_stream_options_clause() %}
    {%- if config.get('stream_options') is not none -%}
        {{ exceptions.raise_compiler_error("stream_options has been renamed to write_stream_options") }}
    {%- endif -%}
    {%- set write_stream_options = config.get('write_stream_options') -%}
    {%- if write_stream_options is none -%}
        {{ return('') }}
    {%- endif -%}
    {%- if not write_stream_options is mapping -%}
        {{ exceptions.raise_compiler_error("write_stream_options must be a dictionary") }}
    {%- endif -%}
    {%- if write_stream_options | length > 0 and config.get('file_format') != 'iceberg' -%}
        {{ exceptions.raise_compiler_error("write_stream_options are supported only for file_format='iceberg'") }}
    {%- endif -%}
    {%- for option, value in write_stream_options.items() -%}
        {%- if option is not string -%}
            {{ exceptions.raise_compiler_error("write_stream_options keys must be strings; got " ~ option) }}
        {%- elif option in ('fanout-enabled', 'check-nullability', 'check-ordering') -%}
            {%- if value is not string or value not in ('true', 'false') -%}
                {{ exceptions.raise_compiler_error("write_stream_options '" ~ option ~ "' must be the string 'true' or 'false'") }}
            {%- endif -%}
        {%- elif not option.startswith('snapshot-property.') or option == 'snapshot-property.' -%}
            {{ exceptions.raise_compiler_error("Unsupported write_stream_options key '" ~ option ~ "'. Supported keys are fanout-enabled, check-nullability, check-ordering, and snapshot-property.<key>.") }}
        {%- elif value is not string -%}
            {{ exceptions.raise_compiler_error("write_stream_options '" ~ option ~ "' must have a string value") }}
        {%- endif -%}
                .option("{{ option }}", "{{ spark__escape_single_quotes(value) }}")
    {%- endfor -%}
{% endmacro %}


{% macro spark__read_stream_options_clause(reader) %}
    {%- set read_stream_options = config.get('read_stream_options') -%}
    {%- if read_stream_options is none -%}
        {{ return('') }}
    {%- endif -%}
    {%- if not read_stream_options is mapping -%}
        {{ exceptions.raise_compiler_error("read_stream_options must be a dictionary") }}
    {%- endif -%}
    {%- for option, value in read_stream_options.items() -%}
        {%- if option is not string -%}
            {{ exceptions.raise_compiler_error("read_stream_options keys must be strings; got " ~ option) }}
        {%- elif option not in ('stream-from-timestamp', 'streamFromTimestamp', 'start-snapshot-id', 'startSnapshotId', 'end-snapshot-id', 'endSnapshotId', 'startingVersion', 'split-size', 'splitSize', 'streaming-skip-delete-snapshots', 'streamingSkipDeleteSnapshots', 'streaming-skip-overwrite-snapshots', 'streamingSkipOverwriteSnapshots', 'streaming-max-files-per-micro-batch', 'streamingMaxFilesPerMicroBatch', 'streaming-max-rows-per-micro-batch', 'streamingMaxRowsPerMicroBatch') -%}
            {{ exceptions.raise_compiler_error("Unsupported read_stream_options key '" ~ option ~ "'. Supported keys are stream-from-timestamp, streamFromTimestamp, start-snapshot-id, startSnapshotId, end-snapshot-id, endSnapshotId, startingVersion, split-size, splitSize, streaming-skip-delete-snapshots, streamingSkipDeleteSnapshots, streaming-skip-overwrite-snapshots, streamingSkipOverwriteSnapshots, streaming-max-files-per-micro-batch, streamingMaxFilesPerMicroBatch, streaming-max-rows-per-micro-batch, and streamingMaxRowsPerMicroBatch.") }}
        {%- elif value is not string -%}
            {{ exceptions.raise_compiler_error("read_stream_options '" ~ option ~ "' must have a string value") }}
        {%- elif option in ('streaming-skip-delete-snapshots', 'streamingSkipDeleteSnapshots', 'streaming-skip-overwrite-snapshots', 'streamingSkipOverwriteSnapshots') and value not in ('true', 'false') -%}
            {{ exceptions.raise_compiler_error("read_stream_options '" ~ option ~ "' must be the string 'true' or 'false'") }}
        {%- elif option == 'startingVersion' and value != 'latest' and not value.isdigit() -%}
            {{ exceptions.raise_compiler_error("read_stream_options 'startingVersion' must be 'latest' or a numeric string") }}
        {%- elif option != 'startingVersion' and option not in ('streaming-skip-delete-snapshots', 'streamingSkipDeleteSnapshots', 'streaming-skip-overwrite-snapshots', 'streamingSkipOverwriteSnapshots') and not value.isdigit() -%}
            {{ exceptions.raise_compiler_error("read_stream_options '" ~ option ~ "' must be a numeric string") }}
        {%- endif -%}
        {{ reader }} = {{ reader }}.option("{{ option }}", "{{ spark__escape_single_quotes(value) }}")
    {%- endfor -%}
{% endmacro %}
