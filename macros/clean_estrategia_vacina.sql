{% macro clean_estrategia_vacina(val) %}
case
  when {{ process_null(val) }} is null
    then null

  when {{ remove_accents_upper(val) }} = "SEM REGISTRO NO SISTEMA DE INFORMACAO DE ORIGEM"
    then null

  when {{ remove_accents_upper(val) }} in (
    "VACINA ESCOLAR", "VACINACAO ESCOLAR"
  )
    then "Vacinação escolar"

  -- Alguns vêm sem acento, alguns com; aqui padronizamos
  when {{ remove_accents_upper(val) }} = "INTENSIFICACAO"
    then "Intensificação"
  when {{ remove_accents_upper(val) }} = "POS-EXPOSICAO"
    then "Pós-exposição"
  when {{ remove_accents_upper(val) }} = "PRE-EXPOSICAO"
    then "Pré-exposição"
  when {{ remove_accents_upper(val) }} = "REEXPOSICAO"
    then "Reexposição"

  else {{ capitalize_first_letter(val) }}
end
{% endmacro %}
