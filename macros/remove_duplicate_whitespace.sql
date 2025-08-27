{% macro remove_duplicate_whitespace(text) %}
  REGEXP_REPLACE({{ text }}, r'\s{2,}', ' ')
{% endmacro %}
