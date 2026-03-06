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

  when REGEXP_REPLACE(
    NORMALIZE({{ aux_clean_lote_vacina(val) }}, NFD),
    r"(?i)[^A-Z0-9]",
    ""
  ) in (
    "SEM",
    "NE", "NN", "NI",
    "NINFORMADO", "NAOINFORMADO", "NAOINFORMA",
    "NAOIDENTIFICADO", "LOTENAOIDENTIFICADO", "NAOIDENTIFADO",
    "NAOCONSTA",
    "ILEGIVEL",
    "SEMREGISTRO", "NAOREGISTRADO",
    "SEMVACINA", "EMFALTA",
    "NAOAPLICADA", "NAOFOIAPLICADA",
    "INEXISTENTE",
    "SEMINFORMACAO",
    "LOTE", "SEMLOTE", "SEMONUMERO", "NAOPOSSUILOTE",
    "LOTENAOINFORMADO",
    "RESGATE", "VACINADERESGATE",
    "TRANSCRITO",
    "COPIA",
    "REGISTRODEVACINAANTERIOR",
    "REGISTRODECARTOESPELHOSEMAINFORMAODEFABRICANTE",
    "SEMLOTEINFORMADONACADERNET",
    "SEMLOTEINFORMADONACADERNETA",
    "NAOESTAESCRITONACADERNETA",
    "NAOINFORMADONACADERNETA",
    "NAOREGISTRADONAFICHA",
    "LOTENAOIDENTIFICADONACADERNETA",
    "CAMPANHA", "DOMICILIO", "REFORCO",
    "DTP", "DTPA", "NOLUGARDADTP",
    "BCG", "VOP",
    "FEBREAMARELA", "FAMARELA",
    "DENGUE", "HPV",
    "TRIPLICEVIRAL", "TRIPLICEVARICELA",
    "REALIZADOSCRVARICELA",
    "BIVALENTE", "PENTAVALENTE",
    "VARICELA", "HEPATITEB", "GRIPE",
    "MENINGOCOCICACONJUGADAC",
    "MENINGOCOCICAC", "MENINGOACWY", "MENINGOC", "MNGACWY", "APLICADOACWY",
    "HEXAVALENTEFOISUBSTITUIDA",
    "MATERNIDADE", "CRECHE", "PERINATAL", "PARTICULAR",
    "OOOOOO", "OOOOOOOOO", "SSSSSSS",
    "DELTOIDEESQUERDO", "COXADIREITA",
    "DOSEFRACIONADA",
    "PACIENTECOMPARECEAUNIDADE",
    --- ???
    "ATIVARACOMPATIBILIDADECOMO",
    "ATIVARACOMPATIBILIDADECOMOLEITORDETELA",
    "CMSJOSEPARANHOSFONTENELLE",
    "MATERNIDADELEILADINIZ"
  )
    then null

  -- String só de 0's, só de X's
  when REGEXP_CONTAINS({{ aux_clean_lote_vacina(val) }}, r"(?i)^(([^0-9A-Z]|0)+|([^0-9A-Z]|X)+)$")
    then null

  else {{ aux_clean_lote_vacina(val) }}
end
{% endmacro %}
