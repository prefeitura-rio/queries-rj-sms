{% macro clean_numeric_string(text) %}
regexp_replace({{ text }}, r"[^0-9]", "")
{% endmacro %}
