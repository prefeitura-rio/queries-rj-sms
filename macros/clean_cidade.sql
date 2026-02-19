{% macro clean_cidade(val) %}
case
  -- Nulos comuns ("None", "NULL", etc)
  when {{ process_null(val) }} is null
    then null

  -- Sinônimos de nulo
  when lower(trim({{ val }})) in (
    "ignorado", "ser alterado"
  )
    then null

  -- Textos que com 0 ou 1 letra somente
  when length(
    REGEXP_REPLACE(
      lower(normalize({{ val }}, nfd)),
      r"[^a-z]",
      ""
    )
  ) <= 1  -- tamanhos >=2 permitem ainda 'rj' p.ex.
    then null

  -- Muitos "Estado do Rio de Janeiro" - isso não é uma cidade!!
  when starts_with(lower(trim({{ val }})), "estado")
    then null

  -----------------------
  -- Algumas variações de cidades hardcoded

  -- RIO DE JANEIRO
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "rj", "rio",
    "riodejaneiro",
    --
    "riodenjaneiro",
    -- Às vezes inserem bairros ao invés de cidades
    "campogrande",
    "jacarepagua",
    "realengo",
    "recreiodosbandeirantes",
    "ilhadogovernador"
  )
    then "Rio de Janeiro"

  -- BARRA DO PIRAÍ
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "barradopirai",
    "barrapirai"
  )
    then "Barra do Piraí"

  -- BELFORD ROXO
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "belfordroxo",  -- Belford Roxo
    --
    "broxo",  -- B.Roxo
    "belforroxo",  -- Belfor Roxo
    "belfortroxo",  -- Belfort Roxo
    "befolrdroxo",  -- Befolrd Roxo
    "belsordroxo",  -- Belsord Roxo
    "belfordrocho"  -- Belford Rocho
  )
    then "Belford Roxo"

  -- BÚZIOS
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "buzios",
    "armacaodebuzios",
    "armacaodosbuzios"
  )
    then "Armação dos Búzios"

  -- CACHOEIRAS DE MACACU
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "cachoeirasdemacacu",  -- Cachoeiras de Macacu
    "cachoeirademacacu",  -- Cachoeira de Macacu
    "cachoeirasdomacacu",  -- Cachoeiras do Macacu
    "cachoeiradomacacu",  -- Cachoeira do Macacu
    --
    "ricachoeiramacacu"
  )
    then "Cachoeiras de Macacu"

  -- DUQUE DE CAXIAS
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "caxias",
    "duquecaxias",
    "duquedecaxias",
    --
    "duquedecaixias",
    -- Xerém
    "xerem"
  )
    then "Duque de Caxias"

  -- ITAGUAÍ
  when lower(trim({{ val }})) = "icarai"
    then "Icaraí"

  -- ITAGUAÍ
  when lower(trim({{ val }})) = "itaguai"
    then "Itaguaí"

  -- ITABORAÍ
  when lower(trim({{ val }})) = "itaborai"
    then "Itaboraí"

  -- NILÓPOLIS
  when lower(trim({{ val }})) = "nilopolis"
    then "Nilópolis"

  -- NITERÓI
  when lower(trim({{ val }})) = "niteroi"
    then "Niterói"

  -- NOVA IGUAÇU
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "niguacu", "novaiguacu", "9guacu",
    "novaigacu"
  )
    then "Nova Iguaçu"

  -- MARICÁ
  when lower(trim({{ val }})) = "marica"
    then "Maricá"

  -- MAGÉ
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "mage",
    --
    "piabeta",
    "piabetarj",
    "suruimage"
  )
    then "Magé"

  -- MESQUITA
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "mesquita",
    --
    "riodejaneiromesquita",
    "mesquitarj"
  )
    then "Mesquita"

  -- PETRÓPOLIS
  when lower(trim({{ val }})) = "petropolis"
    then "Petrópolis"

  -- SÃO GONÇALO
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "sgoncalo", "saogoncalo"
  )
    then "São Gonçalo"

  -- S. JOÃO DE MERITI
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "sjmeriti",  -- S J Meriti
    "sjdemeriti",  -- S J de Meriti
    "sjoaomeriti",  -- S Joao Meriti
    "sjoaodemeriti",  -- S Joao de Meriti
    "sjoaodomeriti",  -- S Joao do Meriti
    "saojoaomeriti",  -- Sao Joao Meriti
    "saojoaodemeriti",  -- Sao Joao de Meriti
    "saojoaodomeriti",  -- Sao Joao do Meriti
    --
    "saojoaodemiriti",  -- Sao Joao de Miriti
    "saojoademeriti",  -- Sao Joa de Meriti
    "saojoaodemerti"  -- Sao Joao de Merti
  )
    then "São João de Meriti"

  -- SEROPÉDICA
  when lower(trim({{ val }})) in (
    "seropédia", "seropedica"
  )
    then "Seropédica"

  -- TERESÓPOLIS
  when lower(trim({{ val }})) in (
    "teresopolis",
    "teresolpolis"
  )
    then "Teresópolis"

  -- Outros
  else {{ proper_br(add_tilde_to_saints(val)) }}
end
{% endmacro %}
