{% macro add_tilde_to_saints(val) %}
-- Adiciona til (~) a "São"s que não o tenham
-- Ex.: add_tilde_to_saints("Sao Paulo") -> "São Paulo"
case
  when REGEXP_CONTAINS({{ val }}, r"\bsao\b")
   then REGEXP_REPLACE({{ val }}, r"\bsao\b", "são")
  when REGEXP_CONTAINS({{ val }}, r"\bSao\b")
   then REGEXP_REPLACE({{ val }}, r"\bSao\b", "São")
  when REGEXP_CONTAINS({{ val }}, r"\bSAO\b")
   then REGEXP_REPLACE({{ val }}, r"\bSAO\b", "SÃO")
  else {{ val }}
end
{% endmacro %}
