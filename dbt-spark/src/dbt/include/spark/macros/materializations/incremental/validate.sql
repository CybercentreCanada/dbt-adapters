{% macro dbt_spark_validate_get_file_format(raw_file_format) %}
  {#-- Validate the file format #}

  {% set accepted_formats = ['text', 'csv', 'json', 'jdbc', 'parquet', 'orc', 'hive', 'delta', 'iceberg', 'libsvm', 'hudi'] %}

  {% set invalid_file_format_msg -%}
    Invalid file format provided: {{ raw_file_format }}
    Expected one of: {{ accepted_formats | join(', ') }}
  {%- endset %}

  {% if raw_file_format not in accepted_formats %}
    {% do exceptions.raise_compiler_error(invalid_file_format_msg) %}
  {% endif %}

  {% do return(raw_file_format) %}
{% endmacro %}


{% macro dbt_spark_validate_get_incremental_strategy(raw_strategy, file_format) %}
  {#-- Validate the incremental strategy #}

  {% set invalid_strategy_msg -%}
    Invalid incremental strategy provided: {{ raw_strategy }}
    Expected one of: 'append', 'merge', 'insert_overwrite', 'microbatch'
  {%- endset %}

  {% set invalid_merge_msg -%}
    Invalid incremental strategy provided: {{ raw_strategy }}
    You can only choose this strategy when file_format is set to 'delta' or 'iceberg' or 'hudi'
  {%- endset %}

  {% set invalid_insert_overwrite_endpoint_msg -%}
    Invalid incremental strategy provided: {{ raw_strategy }}
    You cannot use this strategy when connecting via endpoint
    Use the 'append' or 'merge' strategy instead
  {%- endset %}

  {% set invalid_microbatch_file_format_msg -%}
    Invalid file_format provided for the 'microbatch' incremental strategy: {{ file_format }}
    dbt-spark only supports 'microbatch' when file_format is set to 'iceberg'.
    Iceberg's snapshot-isolated INSERT OVERWRITE is required for safe parallel batch execution.
  {%- endset %}

  {% if raw_strategy not in ['append', 'merge', 'insert_overwrite', 'microbatch'] %}
    {% do exceptions.raise_compiler_error(invalid_strategy_msg) %}
  {%-else %}
    {% if raw_strategy == 'merge' and file_format not in ['delta', 'iceberg', 'hudi'] %}
      {% do exceptions.raise_compiler_error(invalid_merge_msg) %}
    {% endif %}
    {% if raw_strategy in ['insert_overwrite', 'microbatch'] and target.endpoint %}
      {% do exceptions.raise_compiler_error(invalid_insert_overwrite_endpoint_msg) %}
    {% endif %}
    {% if raw_strategy == 'microbatch' and file_format != 'iceberg' %}
      {% do exceptions.raise_compiler_error(invalid_microbatch_file_format_msg) %}
    {% endif %}
  {% endif %}

  {% do return(raw_strategy) %}
{% endmacro %}


{% macro dbt_spark_validate_partition_by(partition_by, file_format='iceberg', writer='sql', require_partition=false, materialization='model') %}
  {% if partition_by is none %}
    {% set partitions = [] %}
  {% elif partition_by is string %}
    {% set partitions = [partition_by] %}
  {% elif partition_by is sequence and partition_by is not mapping %}
    {% set partitions = partition_by %}
  {% else %}
    {% do exceptions.raise_compiler_error("Invalid `partition_by` config for " ~ materialization ~ "; expected a column or list of columns and transforms.") %}
  {% endif %}

  {% if partitions | length == 0 and require_partition %}
    {% do exceptions.raise_compiler_error("dbt-spark 'microbatch' incremental strategy requires a non-empty `partition_by` config.") %}
  {% elif partitions | length == 0 %}
    {% do return(none) %}
  {% endif %}

  {% for raw_partition in partitions %}
    {% if raw_partition is not string %}
      {% do exceptions.raise_compiler_error("Each `partition_by` entry for " ~ materialization ~ " must be a string.") %}
    {% endif %}
    {% set partition = raw_partition | trim %}
    {% set invalid_partition_msg -%}
      Invalid {{ file_format }} partition transform '{{ partition }}' for {{ materialization }}.
      Supported transforms are years, months, days, hours, bucket, and truncate.
    {%- endset %}
    {% if not partition %}
      {% do exceptions.raise_compiler_error("Empty `partition_by` entries are invalid for " ~ materialization ~ ".") %}
    {% elif '(' in partition or ')' in partition %}
      {% set function = partition.split('(', 1)[0] | trim | lower %}
      {% if not partition.endswith(')') or partition.count('(') != 1 or partition.count(')') != 1 %}
        {% do exceptions.raise_compiler_error(invalid_partition_msg) %}
      {% endif %}
      {% set arguments = partition[partition.find('(') + 1:-1].split(',') %}
      {% if file_format | lower != 'iceberg' %}
        {% do exceptions.raise_compiler_error("Partition transforms are supported only with file_format='iceberg'; got '" ~ file_format ~ "' for " ~ materialization ~ ".") %}
      {% endif %}
      {% if function in ('years', 'months', 'days', 'hours') %}
        {% if arguments | length != 1 or not arguments[0] | trim %}
          {% do exceptions.raise_compiler_error(invalid_partition_msg) %}
        {% endif %}
      {% elif function == 'bucket' %}
        {% if arguments | length != 2 or not arguments[0] | trim or not arguments[1] | trim %}
          {% do exceptions.raise_compiler_error(invalid_partition_msg) %}
        {% endif %}
        {% set bucket_count = arguments[0] | trim %}
        {% if bucket_count | int <= 0 or bucket_count != (bucket_count | int | string) %}
          {% do exceptions.raise_compiler_error(invalid_partition_msg) %}
        {% endif %}
      {% elif function == 'truncate' %}
        {% if arguments | length != 2 or not arguments[0] | trim or not arguments[1] | trim %}
          {% do exceptions.raise_compiler_error(invalid_partition_msg) %}
        {% endif %}
        {% set length = arguments[0] | trim %}
        {% if length | int <= 0 or length != (length | int | string) %}
          {% do exceptions.raise_compiler_error(invalid_partition_msg) %}
        {% endif %}
        {% if writer != 'sql' %}
          {% do exceptions.raise_compiler_error("The 'truncate' partition transform is not supported by the " ~ writer ~ " writer; Spark DataFrameWriterV2 supports only years, months, days, hours, and bucket transforms.") %}
        {% endif %}
      {% else %}
        {% do exceptions.raise_compiler_error(invalid_partition_msg) %}
      {% endif %}
    {% endif %}
  {% endfor %}
{% endmacro %}
