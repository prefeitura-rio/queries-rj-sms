{% macro remove_duplicate_whitespace(text) %}
  REGEXP_REPLACE(trim({{ text }}), r'\s{2,}', ' ')
{% endmacro %}
