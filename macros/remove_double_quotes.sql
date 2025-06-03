{% macro remove_double_quotes(column_name) %}
    REPLACE({{ column_name }}, '"', '')
{% endmacro %}