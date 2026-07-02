{{
    config(
        materialized = 'table',
        alias        = "equipes",
        tags         = ["subpav", "cnes_aps"],
        partition_by = {
            "field": "data_particao",
            "data_type": "date"
        },
        cluster_by   = ["cnes", "ine", "competencia"]
    )
}}

with equipes as (
  select
    e.*,
    c.competencia_id,
    c.competencia
  from {{ ref('int_subpav_cnes_aps__equipes') }} e
  left join {{ ref('int_subpav_cnes_aps__competencias_legado') }} c
    on c.data_particao = e.data_particao
  where e.cnes is not null
    and e.ine is not null
)

select
  data_particao,
  competencia,
  competencia_id,

  cast(null as int64) as id,
  cast(null as int64) as unidade_id,

  lpad(cast(cnes as string), 7, '0') as cnes,
  lpad(cast(ine as string), 10, '0') as ine,

  safe_cast(cod_area as int64) as cod_area,
  nm_referencia,

  dt_ativacao,
  dt_desativacao,

  coalesce(pop_assist_quilomb, 0) as tp_pop_assist_quilomb,
  coalesce(pop_assist_assent, 0) as tp_pop_assist_assent,
  coalesce(pop_assist_geral, 0) as tp_pop_assist_geral,
  coalesce(pop_assist_escola, 0) as tp_pop_assist_escola,
  coalesce(pop_assist_pronasci, 0) as tp_pop_assist_pronasci,
  coalesce(pop_assist_indigena, 0) as tp_pop_assist_indigena,
  coalesce(pop_assist_ribeirinha, 0) as tp_pop_assist_ribeirinha,
  coalesce(pop_assist_situacao_rua, 0) as tp_pop_assist_situacao_rua,
  coalesce(pop_assist_priv_liberdade, 0) as tp_pop_assist_priv_liberdade,
  coalesce(pop_assist_conflito_lei, 0) as tp_pop_assist_conflito_lei,
  coalesce(pop_assist_adol_conf_lei, 0) as tp_pop_assist_adol_conf_lei,

  coalesce(co_prof_sus_preceptor, '') as co_prof_sus_preceptor,
  dt_atualiza,

  tipo_equipe_id,
  safe_cast(subtipo_equipe_id as int64) as subtipo_equipe_id,
  safe_cast(motivo_desativacao_equipe_id as int64) as motivo_desativacao_equipe_id,
  safe_cast(tipo_desativacao_id as int64) as tipo_desativacao_id,

  seq_equipe,
  tipo_equipe_descricao,
  classificacao_equipe,
  equipe_ativa,

  current_timestamp() as created_at,
  current_timestamp() as updated_at

from equipes
