{% macro spark__filtered_tblproperties(tblproperties) -%}
  {%- if tblproperties is none -%}
    {{ return(none) }}
  {%- endif -%}

  {%- if not tblproperties is mapping -%}
    {{ exceptions.raise_compiler_error("tblproperties must be a dictionary") }}
  {%- endif -%}

  {%- set filtered_tblproperties = {} -%}
  {%- for prop in tblproperties -%}
    {%- if not prop.startswith('polaris_') -%}
      {%- do filtered_tblproperties.update({prop: tblproperties[prop]}) -%}
    {%- endif -%}
  {%- endfor -%}

  {{ return(filtered_tblproperties) }}
{%- endmacro %}


{#-- Dispatch wrappers for cross-adapter compatibility --#}

{% macro apply_tblproperties(relation, tblproperties) %}
  {{ return(adapter.dispatch('apply_tblproperties', 'dbt')(relation, tblproperties)) }}
{% endmacro %}

{% macro unset_tblproperties(relation, keys_to_unset) %}
  {{ return(adapter.dispatch('unset_tblproperties', 'dbt')(relation, keys_to_unset)) }}
{% endmacro %}

{% macro sync_tblproperties(relation, tblproperties) %}
  {{ return(adapter.dispatch('sync_tblproperties', 'dbt')(relation, tblproperties)) }}
{% endmacro %}


{#-- Default no-op implementations for adapters without tblproperties support --#}

{% macro default__apply_tblproperties(relation, tblproperties) -%}
{%- endmacro %}

{% macro default__unset_tblproperties(relation, keys_to_unset) -%}
{%- endmacro %}

{% macro default__sync_tblproperties(relation, tblproperties) -%}
{%- endmacro %}


{#-- Spark implementations --#}

{% macro spark__apply_tblproperties(relation, tblproperties) -%}
  {%- if config.get('file_format') != 'iceberg' -%}
    {{ return(none) }}
  {%- endif -%}
  {%- set tblproperties = spark__filtered_tblproperties(tblproperties) -%}
  {%- if tblproperties is not none and tblproperties | length > 0 -%}
    {% call statement() -%}
      alter table {{ relation }} set tblproperties (
        {%- for prop in tblproperties -%}
        '{{ prop }}' = '{{ spark__escape_single_quotes(tblproperties[prop]) }}'{% if not loop.last %}, {% endif %}
        {%- endfor -%}
      )
    {%- endcall %}
  {%- endif -%}
{%- endmacro %}


{% macro spark__unset_tblproperties(relation, keys_to_unset) -%}
  {%- if config.get('file_format') != 'iceberg' -%}
    {{ return(none) }}
  {%- endif -%}
  {%- if keys_to_unset is not none and keys_to_unset | length > 0 -%}
    {% call statement() -%}
      alter table {{ relation }} unset tblproperties if exists (
        {%- for key in keys_to_unset -%}
        '{{ key }}'{% if not loop.last %}, {% endif %}
        {%- endfor -%}
      )
    {%- endcall %}
  {%- endif -%}
{%- endmacro %}


{% macro spark__sync_tblproperties(relation, tblproperties) -%}
  {%- if config.get('file_format') != 'iceberg' -%}
    {{ return(none) }}
  {%- endif -%}
  {%- set diff = adapter.get_tblproperties_diff(relation, tblproperties) -%}
  {%- if diff is not none -%}
    {%- if diff.set_properties -%}
      {{ log('Tblproperties drift on ' ~ relation ~ ': setting ' ~ diff.set_properties | list | join(', ')) }}
      {%- for key, value in diff.set_properties.items() -%}
        {{ log('  SET ' ~ key ~ ' = ' ~ value) }}
      {%- endfor -%}
      {{ apply_tblproperties(relation, diff.set_properties) }}
    {%- endif -%}
    {%- if diff.unset_properties -%}
      {{ log('Tblproperties drift on ' ~ relation ~ ': unsetting ' ~ diff.unset_properties | join(', ')) }}
      {%- for key in diff.unset_properties -%}
        {{ log('  UNSET ' ~ key) }}
      {%- endfor -%}
      {{ unset_tblproperties(relation, diff.unset_properties) }}
    {%- endif -%}
  {%- endif -%}
{%- endmacro %}
