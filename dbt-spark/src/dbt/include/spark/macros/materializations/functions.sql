{% materialization function, adapter='spark', supported_languages=['sql', 'python'] %}
    {% if adapter.type() != 'spark' %}
        {{ return(materialization_function_default()) }}
    {% endif %}
    {% set target_relation = this.incorporate(type=this.Function) %}
    {% set selected_functions = [] %}
    {% for uid in selected_resources %}
        {% if uid in graph.functions %}
            {% do selected_functions.append(graph.functions[uid]) %}
        {% endif %}
    {% endfor %}
    {% do adapter.validate_spark_function(model, selected_functions) %}
    {{ run_hooks(pre_hooks) }}
    {% set function_config = target_relation.get_function_config(model) %}
    {% set macro_name = target_relation.get_function_macro_name(function_config) %}
    {% set _dispatch = adapter.dispatch %}
    {% set build_value = _dispatch(macro_name, 'dbt')(target_relation) %}
    {{ function_execute_build_sql(build_value, none, target_relation) }}
    {{ run_hooks(post_hooks) }}
    {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}

{% macro spark__scalar_function_sql(target_relation) %}
    {% if adapter.type() != 'spark' %}
        {{ return(default__scalar_function_sql(target_relation)) }}
    {% endif %}
    {{ return(adapter.spark_function_build_value(model)) }}
{% endmacro %}

{% macro spark__scalar_function_python(target_relation) %}
    {% if adapter.type() != 'spark' %}
        {{ exceptions.raise_compiler_error("Python function creation requires an adapter-specific implementation") }}
    {% endif %}
    {{ return(adapter.spark_function_build_value(model)) }}
{% endmacro %}

{% macro spark__function_execute_build_sql(build_value, existing_relation, target_relation) %}
    {% if adapter.type() != 'spark' %}
        {{ return(default__function_execute_build_sql(build_value, existing_relation, target_relation)) }}
    {% endif %}
    {% set response = adapter.register_spark_function(model, build_value) %}
    {% do store_result('main', response=response) %}
{% endmacro %}
