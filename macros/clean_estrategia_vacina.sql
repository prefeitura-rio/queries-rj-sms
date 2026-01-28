{% macro clean_estrategia_vacina(val) %}
case
  when {{ process_null(val) }} is null
    then null

  -- Opções:
  --  -  bloqueio
  --  -  campanha
  --  -  campanha indiscriminada
  --  -  especial
  -- {!} intensificacao
  -- {!} intensificação
  --  -  monitoramento rápido de cobertura vacinal
  -- {!} pos-exposicao
  -- {!} pós-exposição
  -- {!} pre-exposicao
  -- {!} pré-exposição
  -- {!} reexposicao
  -- {!} reexposição
  --  -  rotina
  --  -  vacina escolar

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
