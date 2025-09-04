{% macro aux_remove_prefix(text) %}
  REGEXP_REPLACE(
    {{ text }},
    r'(?i)^(um|uma|uns|umas|o|a|os|as|à|ao|às|aos|de|da|das|do|dos|em|na|no|nas|nos|por|para|com|e|é)\s+',
    ''
  )
{% endmacro %}
