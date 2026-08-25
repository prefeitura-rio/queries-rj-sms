-- Remove sufixos e prefixos de nomes de estabelecimentos
-- Ex. "SMS CF Nome da Unidade AP 51" -> "CF Nome da Unidade"
{% macro estabelecimento_remove_apendices(text) %}
regexp_replace(
  regexp_replace(
    trim({{ text }}),
    r"(?i)^(\s|SMS|SES(\sRJ)?|MS|UFRJ)*\b",
    ""
  ),
  r"(?i)\bAP\s*[0-9]*$",
  ""
)
{% endmacro %}
