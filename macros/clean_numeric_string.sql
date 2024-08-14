{% macro clean_numeric_string(text) %}

    trim(regexp_replace({{ text }}, r'[^0-9]', ''))

{% endmacro %}
