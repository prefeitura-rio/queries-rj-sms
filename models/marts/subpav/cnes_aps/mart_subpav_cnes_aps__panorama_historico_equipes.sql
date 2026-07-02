{{
    config(
        materialized = 'table',
        alias        = "panorama_historico_equipes",
        tags         = ["subpav", "cnes_aps", "panorama"],
        partition_by = {
            "field": "data_particao",
            "data_type": "date"
        },
        cluster_by   = ["competencia", "ano_competencia"]
    )
}}

with competencias as (
  select
    data_particao,
    competencia,
    competencia_id
  from {{ ref('int_subpav_cnes_aps__competencias_legado') }}
),

populacao_municipio as (
    select
        ano,
        populacao
    from {{ ref('raw_basedosdados_br__ibge_populacao_municipio_rio') }}
),


equipes as (
  select
    e.data_particao,
    c.competencia,
    c.competencia_id,

    coalesce(e.is_equipe_aps_historico, 0) as is_equipe_aps_historico,
    coalesce(e.is_esf_panorama_historico, 0) as is_esf_panorama_historico,
    coalesce(e.is_esb, 0) as is_esb,
    coalesce(e.is_enasf, 0) as is_enasf,
    coalesce(e.is_ecr, 0) as is_ecr,
    coalesce(e.is_eacs_panorama_historico, 0) as is_eacs_panorama_historico,
    coalesce(e.is_eap, 0) as is_eap,
    coalesce(e.is_eapp_panorama, 0) as is_eapp_panorama,
    coalesce(e.is_emad_panorama, 0) as is_emad_panorama,
    coalesce(e.is_emap, 0) as is_emap

  from {{ ref('int_subpav_cnes_aps__equipes') }} e
  left join competencias c
    on c.data_particao = e.data_particao
  where e.equipe_ativa = 1
    and coalesce(e.is_municipio_rio, 1) = 1
    and c.competencia is not null
    and e.ine is not null
),

equipes_agg as (
  select
    data_particao,
    competencia,
    competencia_id,

    sum(is_equipe_aps_historico) as APS,
    sum(is_esf_panorama_historico) as ESF,
    sum(is_esb) as ESB,
    sum(is_enasf) as ENASF,
    sum(is_ecr) as ECR,
    sum(is_eacs_panorama_historico) as EACS,

    case
      when competencia >= '2020-05' then sum(is_eap)
      else 0
    end as EAB,

    sum(is_eap) as EAP,
    sum(is_eapp_panorama) as EAPP,
    sum(is_emad_panorama) as EMAD,
    sum(is_emap) as EMAP

  from equipes
  group by
    data_particao,
    competencia,
    competencia_id
),

limite_competencia_cnes as (
  select
    max(competencia) as ultima_competencia_cnes
  from equipes_agg
),

competencias_panorama as (
  select
    c.data_particao,
    c.competencia,
    c.competencia_id,
    cast(substr(c.competencia, 1, 4) as int64) as ano_competencia,

    p.populacao as POPULACAO,
    p.ano as ANO_POPULACAO_REFERENCIA,

    3500 as POP_COBERTA,

    case
      when c.competencia >= '2024-04' then 46
      else 0
    end as ERES

  from competencias c

  cross join limite_competencia_cnes l

  left join populacao_municipio p
    on p.ano <= cast(substr(c.competencia, 1, 4) as int64)

  where c.competencia >= '2015-01'
    and c.competencia <= l.ultima_competencia_cnes

  qualify row_number() over (
    partition by c.competencia
    order by p.ano desc
  ) = 1
),

composicao as (
  select
    data_particao,
    competencia,
    competencia_id,

    coalesce(aps_completa, 0) as aps_completa,
    coalesce(aps_incompleta, 0) as aps_incompleta,
    coalesce(esf_completa, 0) as esf_completa,
    coalesce(esf_incompleta, 0) as esf_incompleta,
    coalesce(esb_completa, 0) as esb_completa,
    coalesce(esb_incompleta, 0) as esb_incompleta,

    coalesce(inconsistente, 0) as inconsistente

  from {{ ref('int_subpav_cnes_aps__composicao_equipes') }}
),

composicao_agg as (
  select
    data_particao,
    competencia,
    competencia_id,

    sum(aps_completa) as APS_COMPLETA,
    sum(aps_incompleta) as APS_INCOMPLETA,
    sum(esf_completa) as ESF_COMPLETA,
    sum(esf_incompleta) as ESF_INCOMPLETA,
    sum(esb_completa) as ESB_COMPLETA,
    sum(esb_incompleta) as ESB_INCOMPLETA,
    sum(inconsistente) as INCONSISTENTE

  from composicao
  group by
    data_particao,
    competencia,
    competencia_id
),

final as (
  select
    cp.data_particao,
    cp.competencia,
    cast(substr(cp.competencia, 1, 4) as int64) as ano_competencia,

    coalesce(p.APS, 0) as APS,
    coalesce(p.ESF, 0) as ESF,
    coalesce(p.ESB, 0) as ESB,
    coalesce(p.ENASF, 0) as ENASF,
    coalesce(p.ECR, 0) as ECR,
    coalesce(p.EACS, 0) as EACS,
    coalesce(p.EAB, 0) as EAB,
    coalesce(p.EAP, 0) as EAP,
    coalesce(p.EAPP, 0) as EAPP,
    coalesce(p.EMAD, 0) as EMAD,
    coalesce(p.EMAP, 0) as EMAP,

    coalesce(c.INCONSISTENTE, 0) as INCONSISTENTE,

    cast(cp.POPULACAO as string) as POPULACAO,
    cp.ANO_POPULACAO_REFERENCIA,

    coalesce(c.APS_COMPLETA, 0) as APS_COMPLETA,
    coalesce(c.APS_INCOMPLETA, 0) as APS_INCOMPLETA,
    coalesce(c.ESF_COMPLETA, 0) as ESF_COMPLETA,
    coalesce(c.ESF_INCOMPLETA, 0) as ESF_INCOMPLETA,
    coalesce(c.ESB_COMPLETA, 0) as ESB_COMPLETA,
    coalesce(c.ESB_INCOMPLETA, 0) as ESB_INCOMPLETA,

    cp.competencia_id as COMPETENCIA_ID,
    cp.ERES,

    format('%.2f', safe_divide(coalesce(p.APS, 0) * cp.POP_COBERTA, cp.POPULACAO) * 100) as COBERTURA_APS,
    format('%.2f', safe_divide(coalesce(c.ESF_COMPLETA, 0) * cp.POP_COBERTA, cp.POPULACAO) * 100) as COBERTURA_ESF_COMPLETA,
    format('%.2f', safe_divide(coalesce(p.ESF, 0) * cp.POP_COBERTA, cp.POPULACAO) * 100) as COBERTURA_ESF,
    format('%.2f', safe_divide(coalesce(c.ESF_COMPLETA, 0) * cp.POP_COBERTA, cp.POPULACAO) * 100) as COBERTURA_ESF_COMPLETA_GRAFICO,
    format('%.2f', safe_divide(coalesce(p.ESF, 0) * cp.POP_COBERTA, cp.POPULACAO) * 100) as COBERTURA_ESF_GRAFICO,
    current_timestamp() as loaded_at

  from competencias_panorama cp
  left join equipes_agg p
    on p.data_particao = cp.data_particao
    and p.competencia = cp.competencia
    and p.competencia_id = cp.competencia_id
  left join composicao_agg c
    on c.data_particao = cp.data_particao
    and c.competencia = cp.competencia
    and c.competencia_id = cp.competencia_id
)

select *
from final
