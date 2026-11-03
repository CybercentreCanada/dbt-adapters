{% materialization incremental, adapter='spark', supported_languages=['sql', 'python'] -%}
  {% do spark__validate_streaming_options_config('incremental') %}
  {#-- Validate early so we don't run SQL if the file_format + strategy combo is invalid --#}
  {%- set raw_file_format = config.get('file_format') or 'iceberg' -%}
  {%- set raw_strategy = config.get('incremental_strategy') or 'append' -%}
  {%- set grant_config = config.get('grants') -%}

  {%- set file_format = dbt_spark_validate_get_file_format(raw_file_format) -%}
  {%- set strategy = dbt_spark_validate_get_incremental_strategy(raw_strategy, file_format) -%}

  {#-- Set vars --#}

  {%- set unique_key = config.get('unique_key', none) -%}
  {%- set partition_by = config.get('partition_by', none) -%}
  {%- set language = model['language'] -%}
  {% do dbt_spark_validate_partition_by(partition_by, file_format, 'python' if language == 'python' else 'sql', strategy == 'microbatch', 'incremental') %}
  {%- set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') -%}
  {%- set incremental_predicates = config.get('predicates', none) or config.get('incremental_predicates', none) -%}
  {%- set target_relation = this -%}
  {%- set existing_relation = load_relation(this) -%}
  {%- set write_order_enabled = adapter.write_order_enabled(existing_relation, model.config) -%}
  {% if write_order_enabled %}
    {% set target_relation = target_relation.incorporate(type='table', is_iceberg=true) %}
    {% if existing_relation is not none %}
      {% set existing_relation = existing_relation.incorporate(is_iceberg=true) %}
    {% endif %}
  {% endif %}

  {#--
    Metadata mutations (partition check, tblproperties, comments) race when microbatch
    batches run in parallel. dbt-core always runs batch 0 sequentially before any
    concurrent batch, so only the first claimer per invocation syncs metadata.
  --#}
  {%- set sync_metadata = (not model.batch) or adapter.claim_metadata_sync(target_relation, invocation_id) -%}

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
    Controlled by the `set_partition_overwrite_mode` behavior flag (default: true).
    On Iceberg this is a no-op (Iceberg's INSERT OVERWRITE uses native
    dynamic-partition semantics via the Iceberg SQL extensions), but we emit
    it defensively so the session is always in the expected state.
    For non-Iceberg formats (parquet, delta, hive, etc.) this SET is required
    for correct dynamic partition overwrite behavior.
  --#}
  {%- if strategy in ('insert_overwrite', 'microbatch') and partition_by and adapter.behavior.set_partition_overwrite_mode -%}
    {%- call statement() -%}
      set spark.sql.sources.partitionOverwriteMode = DYNAMIC
    {%- endcall -%}
  {%- endif -%}

  {#-- Run pre-hooks --#}
  {{ run_hooks(pre_hooks) }}

  {#-- Incremental run logic --#}
  {%- if existing_relation is none -%}
    {#-- Relation must be created --#}
    {% if write_order_enabled %}
      {% if language == 'python' %}
        {% call statement('main', language='python') %}
          {{ py_write_table(compiled_code, target_relation, false, true, false) }}
        {% endcall %}
      {% else %}
        {% do spark_materialize_write_order_sql(target_relation, compiled_code, false) %}
      {% endif %}
      {% set target_relation = adapter.get_write_order_relation(target_relation) %}
    {% else %}
    {%- call statement('main', language=language) -%}
      {{ create_table_as(False, target_relation, compiled_code, language) }}
    {%- endcall -%}
    {% endif %}
    {% do persist_constraints(target_relation, model) %}
    {% do apply_tblproperties(target_relation, config.get('tblproperties')) %}
  {%- elif existing_relation.is_view or should_full_refresh() -%}
    {#-- Relation must be dropped & recreated --#}
    {% set is_delta = (file_format == 'delta' and existing_relation.is_delta) %}
    {% if not write_order_enabled and not is_delta %} {#-- Atomic Iceberg replacements must not drop first. --#}
      {% do adapter.drop_relation(existing_relation) %}
    {% endif %}
    {% if write_order_enabled %}
      {% if language == 'python' %}
        {% call statement('main', language='python') %}
          {{ py_write_table(compiled_code, target_relation, false, true, true) }}
        {% endcall %}
      {% else %}
        {% do spark_materialize_write_order_sql(target_relation, compiled_code, true) %}
      {% endif %}
      {% set target_relation = adapter.get_write_order_relation(target_relation) %}
    {% else %}
    {%- call statement('main', language=language) -%}
      {{ create_table_as(False, target_relation, compiled_code, language) }}
    {%- endcall -%}
    {% endif %}
    {% do persist_constraints(target_relation, model) %}
    {% do apply_tblproperties(target_relation, config.get('tblproperties')) %}
  {%- else -%}
    {#-- Relation must be merged --#}
    {% set evolve_write_order = write_order_enabled and on_schema_change in ('append_new_columns', 'sync_all_columns') %}
    {%- if sync_metadata and not evolve_write_order -%}
      {% if write_order_enabled %}
        {% set target_relation = adapter.sync_write_order(target_relation, model.config) %}
      {% endif %}
      {% do adapter.check_partition_sync(target_relation, config.get('file_format'), config.get('partition_by')) %}
      {% do sync_tblproperties(target_relation, config.get('tblproperties')) %}
    {%- endif -%}
    {%- call statement('create_tmp_relation', language=language) -%}
      {#-- CCCS --#}
      {{ create_table_as(use_temporary_view, tmp_relation, compiled_code, language) }}
    {%- endcall -%}
    {% if sync_metadata and evolve_write_order %}
      {% do adapter.validate_write_order_schema(target_relation, tmp_relation, model.config) %}
      {% do adapter.check_partition_sync(target_relation, config.get('file_format'), config.get('partition_by')) %}
      {% do sync_tblproperties(target_relation, config.get('tblproperties')) %}
    {% endif %}
    {%- do process_schema_changes(on_schema_change, tmp_relation, existing_relation) -%}
    {% if sync_metadata and evolve_write_order %}
      {% set target_relation = adapter.sync_write_order(target_relation, model.config) %}
    {% endif %}
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

  {% if sync_metadata %}
    {% do persist_docs(target_relation, model) %}
  {% endif %}

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
