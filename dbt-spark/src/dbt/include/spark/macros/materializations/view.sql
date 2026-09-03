{% materialization view, adapter='spark' -%}
    {% do spark__validate_streaming_options_config('view') %}
    {{ return(create_or_replace_view()) }}
{%- endmaterialization %}
