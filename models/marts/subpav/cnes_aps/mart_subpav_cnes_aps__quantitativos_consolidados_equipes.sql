{{ 
    config(
        materialized = 'table',
        alias        = "quantitativos_consolidados_equipes",
        tags         = ["subpav", "cnes_aps"],
        partition_by = {
            "field": "data_particao",
            "data_type": "date"
        },
        cluster_by   = ["competencia", "cnes", "ine"]
    ) 
}}


with composicao as (
  select
    data_particao,
    competencia,
    competencia_id,

    cnes,
    ine,
    nm_referencia,

    tipo_equipe_id,

    desligados_60_medico,
    desligados_60_enfermeiro,
    desligados_60_tec_aux_enfermagem,
    desligados_60_acs,
    desligados_60_cirurgiao_dentista,
    desligados_60_aux_bucal,
    desligados_60_tec_bucal,

    total_medico_40h,
    total_medico_20h,
    total_enfermeiro_40h,
    total_tec_aux_enfermagem_40h,
    total_acs_40h,

    cirurgiao,
    cirurgiao_20,
    aux_bucal,
    aux_bucal_20,
    tec_bucal,
    tec_bucal_20,

    aps_completa,
    esb_completa,
    esf_completa,

    inconsistente_qpe,
    data_eqp_incompleta,
    data_eqp_vacancia

  from {{ ref('int_subpav_cnes_aps__composicao_equipes') }}
)

select
  data_particao,
  competencia,
  competencia_id,

  lpad(cast(cnes as string), 7, '0') as cnes,
  lpad(cast(ine as string), 10, '0') as ine,
  nm_referencia,

  cast(null as int64) as unidade_id,
  cast(null as int64) as equipe_id,
  tipo_equipe_id,

  coalesce(desligados_60_medico, 0) as desligados_60_med,
  coalesce(desligados_60_enfermeiro, 0) as desligados_60_enf,
  coalesce(desligados_60_tec_aux_enfermagem, 0) as desligados_60_aux,
  coalesce(desligados_60_acs, 0) as desligados_60_acs,
  coalesce(desligados_60_cirurgiao_dentista, 0) as desligados_60_dent,
  coalesce(desligados_60_aux_bucal, 0) as desligados_60_aux_bucal,
  coalesce(desligados_60_tec_bucal, 0) as desligados_60_tec_bucal,

  coalesce(total_medico_40h, 0) as medico,
  coalesce(total_medico_20h, 0) as medico_20,
  coalesce(total_enfermeiro_40h, 0) as enfermeiro,
  coalesce(total_tec_aux_enfermagem_40h, 0) as tec_aux,
  coalesce(total_acs_40h, 0) as acs,

  coalesce(cirurgiao, 0) as cirurgiao,
  coalesce(cirurgiao_20, 0) as cirurgiao_20,
  coalesce(aux_bucal, 0) as aux_bucal,
  coalesce(aux_bucal_20, 0) as aux_bucal_20,
  coalesce(tec_bucal, 0) as tec_bucal,
  coalesce(tec_bucal_20, 0) as tec_bucal_20,

  coalesce(aps_completa, 0) as aps_completa,
  coalesce(esb_completa, 0) as esb_completa,
  coalesce(esf_completa, 0) as esf_completa,

  coalesce(inconsistente_qpe, 0) as inconsistente,

  data_eqp_incompleta,
  data_eqp_vacancia,

  current_timestamp() as loaded_at

from composicao
