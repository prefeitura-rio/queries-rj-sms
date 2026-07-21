{{
    config(
        schema="app_historico_clinico",
        alias="contrarreferencia",
        materialized="table",
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        },
    )
}}

with source as (
  select

    cr.id_hci,

    cr.estabelecimento.nome as estabelecimento,

    cr.profissional.nome as profissional_nome,
    INITCAP(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(lower(cr.profissional.cargo), r"^medico\s*", ""),
            r"\bcirurgiao\b",
            "cirurgião"
          ),
          r"\bfonoaudiologo\b",
          "fonoaudiólogo"
        ),
        r"\b\s+[e\-]\s+\b",  -- De "xxxx e xxxx" ou "xxxx - xxxx"
        "/"                  -- Para "xxxx/xxxx"
      )
    ) as profissional_cargo,

    cr.contrarreferencia.numero as documento_numero,
    cr.contrarreferencia.datahora as documento_datahora,
    cr.contrarreferencia.pdf_uri as documento_uri,

    cr.avaliacao.conduta,
    cr.avaliacao.seguimento,
    -- Só queremos passar resumo se os outros campos
    -- não foram automaticamente detectados
    if(
      cr.avaliacao.historia_doenca_atual is null
      and cr.avaliacao.medicamentos_em_uso is null
      and cr.avaliacao.hipotese_diagnostica is null,
      cr.avaliacao.resumo,
      null
    ) as resumo,
    cr.avaliacao.historia_doenca_atual,
    cr.avaliacao.medicamentos_em_uso,
    cr.avaliacao.hipotese_diagnostica,

    safe_cast(cr.paciente.cpf as int64) as cpf_particao

  from {{ ref("mart_historico_clinico__contrarreferencia") }} as cr
),
suspeita_hiv as (
  select *
  from source
  where 1=1
  {% for field in [
    "conduta", "seguimento",
    "resumo", "historia_doenca_atual",
    "hipotese_diagnostica"
  ] %}
  and not ifnull(
    -- Encontra casos com menção a HIV/termos correlatos
    regexp_contains(
      lower({{ field }}),
      -- B20-B24 são CIDs de HIV
      -- PVHIV - Pessoa Vivendo com HIV
      -- CD4 - Linfócito referência em testes de HIV
      -- TARV - Terapia Antirretroviral
      -- Tenofovir - Nome de medicamento antirretroviral
      r"\b(b2[0-4][\s\-\.]*[0-9]?|(pv)?hiv|aids|imuno[\s\-]*defici[eê]ncia|cd4|tarv|tenofovir)\b"
    )
    -- ...exceto se também tiverem menção a teste negativo/não reagente:
    -- - teste rápido xx/24: hiv neg; sifilis neg; anti hcv neg; hbsag neg
    -- - lab xx/28: hbsag neg; hcv tr neg; hiv neg; vdrl neg
    -- - .../ anti-hcv: não reagente/ anti-hiv: não reagente/ hbsg: não reagente/...
    and not regexp_contains(
      lower({{ field }}),
      r"\b(anti[\s\-]*)?hiv[\s\-\:]*\b(neg|nega\s*tivos?|n([aã]o)?[\s\-]*rea?gentes?|nr)\b"
    )
    -- - teste rapido hiv hep b hep c sifilis nega tivos xxx 23
    -- - teste rapido (xx/xx/2023): hiv/ sifilis/ hepatite b,c: não reagentes
    and not regexp_contains(
      lower({{ field }}),
      r"\b(testes?\s*r[aá]pidos|tr)?.{0,50}hiv.{0,50}\b(neg|nega\s*tivos?|n([aã]o)?[\s\-]*re?agentes?|nr)\b"
    )
    -- - teste rápido (xx/2023): negativo para hiv, sífilis, hep b e c
    -- - testes rapidos negativos para hep b e c , sifilis e hiv
    and not regexp_contains(
      lower({{ field }}),
      r"\b(testes?\s*r[aá]pidos|tr)?.{0,50}\b(neg|nega\s*tivos?|n([aã]o)?[\s\-]*rea?gentes?|nr)\b\s*(p|pra|para)?\b.{0,50}hiv\b"
    )
    -- - sorologias negativas para hiv, hepatite b e c
    -- ...ou pedidos ainda não realizados:
    -- - cd: 1- solicito lac c/ sorologias virais hiv + hepatites;
    and not regexp_contains(
      lower({{ field }}),
      r"\bsorologias?(\s*vira(l|is))?(\s*negativas?)?(\s*(p|pra|para))?\s*hiv\b"
    )
    -- - necessita: lab anti-hiv
    and not regexp_contains(
      lower({{ field }}),
      r"\b(necessita|aguard(o|ando)|solicit(o|ar?|ados?))([:,\s\-]|(incluindo|novamente|de\s*novo|lab))*\b(anti[\s\-]*)?hiv\b"
    ),
    -- ifnull(..., false); se o campo é nulo, queremos que
    -- ele seja `false` = não tem referência a HIV
    false
  )
  {% endfor %}
)

select *
from suspeita_hiv
