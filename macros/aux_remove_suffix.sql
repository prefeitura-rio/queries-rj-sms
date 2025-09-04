{% macro aux_remove_suffix(text) %}
  REGEXP_REPLACE(
    {{ text }},
    r'(?i)\s*\(?alegad(o|a)\)?$',
    ''
  )
{% endmacro %}
