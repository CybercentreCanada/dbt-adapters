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



{% macro apply_tblproperties(relation, tblproperties) -%}
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