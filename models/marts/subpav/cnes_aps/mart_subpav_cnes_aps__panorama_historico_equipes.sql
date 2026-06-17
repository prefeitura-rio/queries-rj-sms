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

parametros as (
  select
    e.*,

    case
      when e.competencia between '2017-01' and '2017-12' then 6498837
      when e.competencia between '2018-01' and '2018-12' then 6520266
      when e.competencia between '2019-01' and '2019-12' then 6688927
      when e.competencia between '2020-01' and '2021-11' then 6718903
      when e.competencia between '2021-12' and '2023-12' then 6775561
      when e.competencia between '2024-01' and '2024-12' then 6211223
      when e.competencia >= '2025-01' then 6729894
    end as POPULACAO,

    3500 as POP_COBERTA,

    case
      when e.competencia >= '2024-04' then 46
      else 0
    end as ERES

  from equipes_agg e
),

final as (
  select
    p.data_particao,
    p.competencia,
    cast(substr(p.competencia, 1, 4) as int64) as ano_competencia,

    p.APS,
    p.ESF,
    p.ESB,
    p.ENASF,
    p.ECR,
    p.EACS,
    p.EAB,
    p.EAP,
    p.EAPP,
    p.EMAD,
    p.EMAP,

    coalesce(c.INCONSISTENTE, 0) as INCONSISTENTE,

    cast(p.POPULACAO as string) as POPULACAO,

    coalesce(c.APS_COMPLETA, 0) as APS_COMPLETA,
    coalesce(c.APS_INCOMPLETA, 0) as APS_INCOMPLETA,
    coalesce(c.ESF_COMPLETA, 0) as ESF_COMPLETA,
    coalesce(c.ESF_INCOMPLETA, 0) as ESF_INCOMPLETA,
    coalesce(c.ESB_COMPLETA, 0) as ESB_COMPLETA,
    coalesce(c.ESB_INCOMPLETA, 0) as ESB_INCOMPLETA,

    p.competencia_id as COMPETENCIA_ID,
    p.ERES,

    format('%.2f', safe_divide(p.APS * p.POP_COBERTA, p.POPULACAO) * 100) as COBERTURA_APS,
    format('%.2f', safe_divide(coalesce(c.ESF_COMPLETA, 0) * p.POP_COBERTA, p.POPULACAO) * 100) as COBERTURA_ESF_COMPLETA,
    format('%.2f', safe_divide(p.ESF * p.POP_COBERTA, p.POPULACAO) * 100) as COBERTURA_ESF,
    format('%.2f', safe_divide(coalesce(c.ESF_COMPLETA, 0) * p.POP_COBERTA, p.POPULACAO) * 100) as COBERTURA_ESF_COMPLETA_GRAFICO,
    format('%.2f', safe_divide(p.ESF * p.POP_COBERTA, p.POPULACAO) * 100) as COBERTURA_ESF_GRAFICO,

    current_timestamp() as loaded_at

  from parametros p
  left join composicao_agg c
    on c.data_particao = p.data_particao
    and c.competencia_id = p.competencia_id

  order by
  p.data_particao desc,
  p.competencia desc

)

select *
from final
