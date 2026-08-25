{{ 
  config(
      schema = "intermediario_cdi",
      alias  = "judicial_residual",
      materialized = "table",
      meta={"owner": "karen"}
) }}

with judicial_residual_2025 as (

  select
    processo_rio,
    envolve_mrj,
    oficio,
    data_oficio_origem,
    demandante,
    orgao,
    processo,
    assunto,
    solicitacao,
    area,
    sexo,
    idade,
    entrada_gat3,
    prazo_dias,
    vencimento,
    data_saida,
    orgao_para_subsidiar,
    retorno,
    no_oficio,
    data_oficio,
    data_pg_pas_dta_sfc,
    cast(null as date) as data_conclusao,
    observacoes,
    situacao

  from {{ ref('raw_cdi__judicial_residual_2025') }}

),

judicial_residual_2026 as (

  select
    processo_rio,
    envolve_mrj,
    oficio,
    data_oficio_origem,
    demandante,
    orgao,
    processo,
    assunto,
    solicitacao,
    area,
    sexo,
    idade,
    entrada_gat3,
    prazo_dias,
    vencimento,
    data_saida,
    orgao_para_subsidiar,
    retorno,
    no_oficio,
    data_oficio,
    data_pg_pas_dta_sfc,
    data_conclusao,
    observacoes,
    situacao

  from {{ ref('raw_cdi__judicial_residual_2026') }}

),

src as (

  select * from judicial_residual_2025

  union all

  select * from judicial_residual_2026

),

base as (

  select *
  from src

  where not (
    entrada_gat3 is null
    and processo_rio is null
    and processo is null
    and oficio is null
  )

),

calc as (

  select
    case
      when processo_rio is not null
        and upper(processo_rio) != 'E-MAIL'
        then processo_rio

      when no_oficio is not null
        then no_oficio

      when oficio is not null
        then oficio

      when processo is not null
        then processo

      else concat(
        'SEM_ID_',
        cast(entrada_gat3 as string), '_',
        regexp_replace(coalesce(solicitacao, 'sem_solicitacao'), r'\s+', '_'), '_',
        regexp_replace(coalesce(orgao_para_subsidiar, 'sem_orgao'), r'\s+', '_')
      )
    end as id,

    processo_rio,
    envolve_mrj,
    oficio,
    demandante,
    orgao,
    processo,
    assunto,

    case
      when lower(solicitacao) = 'exames' then 'exame'
      else lower(regexp_replace(solicitacao, r'\s*,\s*', ', '))
    end as solicitacao,

    area,
    sexo,
    idade,

    case
      when idade is null then null
      when regexp_contains(lower(idade), r'\d') then null
      when regexp_contains(lower(idade), r'idos.*adult|adult.*idos') then 'adulto e idoso'
      when regexp_contains(lower(idade), r'\brn\b|rec[eé]m[- ]?nascid') then 'rn'
      when regexp_contains(lower(idade), r'idos') then 'idoso'
      when regexp_contains(lower(idade), r'adult') then 'adulto'
      when regexp_contains(lower(idade), r'crian[cç]a') then 'crianca'
      when regexp_contains(lower(idade), r'adolesc') then 'adolescente'
      when regexp_contains(lower(idade), r'n[uú]cleo\s+familiar|nucleo\s+familiar|fam[ií]li') then 'nucleo_familiar'
      else null
    end as idade_categoria,

    prazo_dias,
    orgao_para_subsidiar,
    no_oficio,
    observacoes,
    situacao,

    data_oficio_origem,
    entrada_gat3,
    vencimento,
    retorno,
    data_saida,
    data_oficio,
    data_pg_pas_dta_sfc,
    data_conclusao

  from base

)

select *
from calc