{% macro spark__generate_alias_name(custom_alias_name=none, node=none) %}
  {% if node is not none and node.resource_type == 'model' %}
    {% do adapter.validate_write_order_config(node.config, node.name) %}
  {% endif %}
  {{ return(default__generate_alias_name(custom_alias_name, node)) }}
{% endmacro %}

{% macro spark_materialize_write_order_sql(target_relation, compiled_code, exists) %}
  {{ return(adapter.dispatch('materialize_write_order_sql', 'dbt')(target_relation, compiled_code, exists)) }}
{% endmacro %}

{% macro spark__materialize_write_order_sql(target_relation, compiled_code, exists) %}
    {% if config.get('contract').enforced %}
      {% do get_assert_columns_equivalent(compiled_code) %}
      {% set compiled_code = get_select_subquery(compiled_code) %}
    {% endif %}
    {% set clauses %}
      {{ file_format_clause() }}
      {{ options_clause() }}
      {{ partition_cols(label="partitioned by") }}
      {{ clustered_cols(label="clustered by") }}
      {{ comment_clause() }}
    {% endset %}
    {% set response = adapter.materialize_write_order_sql(target_relation, compiled_code, clauses, location_clause(), model.config, exists) %}
    {% do store_result('main', response=response, agate_table=none) %}
    {% do write(compiled_code) %}
{% endmacro %}
