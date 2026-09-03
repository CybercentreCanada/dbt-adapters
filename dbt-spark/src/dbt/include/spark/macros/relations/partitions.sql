{% macro fetch_partition_spec(relation) -%}
  {% call statement('show_create_table', fetch_result=True) -%}
    SHOW CREATE TABLE {{ relation }}
  {%- endcall %}
  {% do return(load_result('show_create_table').table) %}
{%- endmacro %}
