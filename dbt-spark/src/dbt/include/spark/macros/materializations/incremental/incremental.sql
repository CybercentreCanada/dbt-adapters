{% materialization incremental, adapter='spark', supported_languages=['sql', 'python'] -%}
  {#-- Validate early so we don't run SQL if the file_format + strategy combo is invalid --#}
  {%- set raw_file_format = config.get('file_format', default='parquet') -%}
  {%- set raw_strategy = config.get('incremental_strategy') or 'append' -%}
  {%- set grant_config = config.get('grants') -%}

  {%- set file_format = dbt_spark_validate_get_file_format(raw_file_format) -%}
  {%- set strategy = dbt_spark_validate_get_incremental_strategy(raw_strategy, file_format) -%}

  {#-- Set vars --#}

  {%- set unique_key = config.get('unique_key', none) -%}
  {%- set partition_by = config.get('partition_by', none) -%}
  {%- set language = model['language'] -%}
  {%- set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') -%}
  {%- set incremental_predicates = config.get('predicates', none) or config.get('incremental_predicates', none) -%}
  {%- set target_relation = this -%}
  {%- set existing_relation = load_relation(this) -%}

  {#--
    Build the tmp relation identifier with a per-batch suffix so concurrent
    microbatch batches do not clobber each other's temp relations.
    Mirrors the convention used by the base `make_temp_relation` macro at
    dbt-adapters/src/dbt/include/global_project/macros/adapters/relation.sql
    so that `model.batch.id` is appended when running inside a microbatch batch.
  --#}
  {%- set tmp_relation_suffix = '__dbt_tmp' -%}
  {%- if model.batch -%}
    {%- set tmp_relation_suffix = tmp_relation_suffix ~ '_' ~ model.batch.id -%}
  {%- endif -%}
  {% set tmp_relation = this.incorporate(path = {"identifier": this.identifier ~ tmp_relation_suffix}) -%}
  {#-- User hook for redirecting the tmp relation (e.g., to a scratch schema). --#}
  {%- set tmp_relation = spark_resolve_incremental_tmp_relation(tmp_relation) -%}

  {#-- CCCS --#}
  {%- set use_temporary_view = True -%}
  {#-- for SQL model we will create temp view that doesn't have database and schema --#}
  {%- if language == 'sql' -%}
    {%- set tmp_relation = tmp_relation.include(database=false, schema=false) -%}
  {%- endif -%}

  {#-- CCCS when using the spark_session_based_cluster python submission method
     we can create views which can then be used by spark later on.
     Thus here we pass to the py_write_table the temporary flag.
     --#}
  {%- if language == 'python' -%}
    {%- if config.get('submission_method') == 'spark_session_based_cluster' -%}
      {%- set tmp_relation = tmp_relation.include(database=false, schema=false) -%}
    {%- else -%}
      {#-- Other submission method are unable to use temporary views, they create actual tables --#}
      {%- set use_temporary_view = False -%}
    {%- endif -%}
  {%- endif -%}

  {#--
    Set Overwrite Mode.
    For Iceberg, the `spark.sql.sources.partitionOverwriteMode` flag has no
    effect (Iceberg's INSERT OVERWRITE uses native dynamic-partition
    semantics via the Iceberg SQL extensions). Skipping the SET on the
    Iceberg path also removes a session-level race condition that would
    otherwise block safe parallel microbatch execution, because Spark 3.5.6
    does not support multi-statement submissions and the SET cannot be
    co-located with the INSERT.
  --#}
  {%- if strategy == 'insert_overwrite' and partition_by and file_format != 'iceberg' -%}
    {%- call statement() -%}
      set spark.sql.sources.partitionOverwriteMode = DYNAMIC
    {%- endcall -%}
  {%- endif -%}

  {#-- Run pre-hooks --#}
  {{ run_hooks(pre_hooks) }}

  {#-- Incremental run logic --#}
  {%- if existing_relation is none -%}
    {#-- Relation must be created --#}
    {%- call statement('main', language=language) -%}
      {{ create_table_as(False, target_relation, compiled_code, language) }}
    {%- endcall -%}
    {% do persist_constraints(target_relation, model) %}
    {% do apply_tblproperties(target_relation, config.get('tblproperties')) %}
  {%- elif existing_relation.is_view or should_full_refresh() -%}
    {#-- Relation must be dropped & recreated --#}
    {% set is_delta = (file_format == 'delta' and existing_relation.is_delta) %}
    {% if not is_delta %} {#-- If Delta, we will `create or replace` below, so no need to drop --#}
      {% do adapter.drop_relation(existing_relation) %}
    {% endif %}
    {%- call statement('main', language=language) -%}
      {{ create_table_as(False, target_relation, compiled_code, language) }}
    {%- endcall -%}
    {% do persist_constraints(target_relation, model) %}
    {% do apply_tblproperties(target_relation, config.get('tblproperties')) %}
  {%- else -%}
    {#-- Relation must be merged --#}
    {#--
      Per-batch metadata mutations (`check_partition_sync` / `sync_tblproperties`)
      race when multiple microbatch batches run in parallel. Skip them only
      when dbt-core may schedule batches concurrently:

        * Not a microbatch run                -> always safe to sync.
        * `concurrent_batches: false`         -> dbt-core guarantees one batch
                                                  in flight at a time, safe to sync.
        * `concurrent_batches: true` or unset -> potentially parallel, skip.

      See dbt-core's `MicrobatchBatchRunner.should_run_in_parallel` for the
      authoritative resolution logic.
    --#}
    {%- set is_concurrent_microbatch = model.batch and config.get('concurrent_batches') != false -%}
    {%- if not is_concurrent_microbatch -%}
      {% do adapter.check_partition_sync(target_relation, config.get('file_format'), config.get('partition_by')) %}
      {% do sync_tblproperties(target_relation, config.get('tblproperties')) %}
    {%- endif -%}
    {%- call statement('create_tmp_relation', language=language) -%}
      {#-- CCCS --#}
      {{ create_table_as(use_temporary_view, tmp_relation, compiled_code, language) }}
    {%- endcall -%}
    {%- do process_schema_changes(on_schema_change, tmp_relation, existing_relation) -%}
    {%- call statement('main') -%}
      {{ dbt_spark_get_incremental_sql(strategy, tmp_relation, target_relation, existing_relation, unique_key, incremental_predicates) }}
    {%- endcall -%}
    {#-- CCCS Add a check to see if a real table was created if so go ahead and delete this table. --#}
    {%- if language == 'python' and not use_temporary_view -%}
      {#--
      This is yucky.
      See note in dbt-spark/dbt/include/spark/macros/adapters.sql
      re: python models and temporary views.

      Also, why do neither drop_relation or adapter.drop_relation work here?!
      --#}
      {% call statement('drop_relation') -%}
        drop table if exists {{ tmp_relation }}
      {%- endcall %}
    {%- endif -%}
  {%- endif -%}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode) %}
  {% do apply_grants(target_relation, grant_config, should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}


{#--
  User-overridable hook for redirecting the incremental tmp relation
  (for example, to a scratch schema) so that concurrent microbatch
  batches writing into the same target schema do not collide on
  ancillary metadata. Mirrors `snowflake__resolve_incremental_tmp_relation`.
--#}
{% macro spark_resolve_incremental_tmp_relation(tmp_relation) %}
  {{ return(adapter.dispatch('spark_resolve_incremental_tmp_relation', 'dbt')(tmp_relation)) }}
{% endmacro %}

{% macro default__spark_resolve_incremental_tmp_relation(tmp_relation) %}
  {{ return(tmp_relation) }}
{% endmacro %}

{% macro spark__spark_resolve_incremental_tmp_relation(tmp_relation) %}
  {{ return(tmp_relation) }}
{% endmacro %}
