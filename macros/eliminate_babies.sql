-- Muitas ocorrências de "nome social" preenchido como "rn de <nome da mãe>" ou "filho de <...>"
{% macro eliminate_babies(text) %}
  if(
    (
      starts_with(lower({{ text }}), "rn ")
      or starts_with(lower({{ text }}), "filho de ")
      or starts_with(lower({{ text }}), "filha de ")
    ),
    null,
    {{ text }}
  )
{% endmacro %}
