{% macro aux_clean_lote_vacina(val) %}
upper(
  REGEXP_REPLACE(
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        {{ val }},
        r'^([\"\',\.\s\\/\+\-–´`~\[\]%\|!]|\*{2,})+',  -- Lixo no início
        ""
      ),
      r'([\"\',\.\s\\/\+\-–´`~\[\]%\|!]|\*{2,})+$',  -- Lixo no fim
      ""
    ),
    r"\s{1,}",  -- N espaços em branco -> 1 espaço
    " "
  )
)
{% endmacro %}
-- Syntax Highlighting to VS Code fica quebrada se não incluir isso: '
{% macro clean_lote_vacina(val) %}
case
  when {{ process_null(aux_clean_lote_vacina(val)) }} is null
    then null

  when {{ aux_clean_lote_vacina(val) }} in (
    "N/E", "Ñ INFORMADO"
  )
    then null

  -- String só de 0's, só de X's
  when REGEXP_CONTAINS({{ aux_clean_lote_vacina(val) }}, r"(?i)^(([^0-9A-Z]|0)+|([^0-9A-Z]|X)+)$")
    then null

  else {{ aux_clean_lote_vacina(val) }}
end
{% endmacro %}
