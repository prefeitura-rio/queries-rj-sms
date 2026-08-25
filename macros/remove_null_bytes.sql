{% macro remove_null_bytes(text) %}
    regexp_replace({{ text }}, r'\x00', '')
{% endmacro %}
