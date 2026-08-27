{{
    config(
        materialized = 'table',
        alias        = "equipes_profissionais",
        tags         = ["subpav", "cnes_aps"],
        partition_by = {
            "field": "data_particao",
            "data_type": "date"
        },
        cluster_by   = ["ine", "cpf", "cod_cbo", "competencia"]
    )
}}

with equipes_profissionais as (
  select
    ep.*,
    c.competencia_id,
    c.competencia
  from {{ ref('int_subpav_cnes_aps__equipes_profissionais') }} ep
  left join {{ ref('int_subpav_cnes_aps__competencias_legado') }} c
    on c.data_particao = ep.data_particao
  where ep.cpf is not null
    and ep.ine is not null
    and ep.cod_cbo is not null
)

select
  data_particao,
  competencia,
  competencia_id,

  cast(null as int64) as id,
  cast(null as int64) as equipe_id,
  cast(null as int64) as profissional_id,
  cast(null as int64) as cbo_id,

  lpad(cast(cpf as string), 11, '0') as cpf,
  lpad(cast(cnes as string), 7, '0') as cnes,
  lpad(cast(ine as string), 10, '0') as ine,
  cod_cbo,

  dt_entrada,
  dt_desligamento,
  coalesce(possui_dt_desligamento, 0) as possui_dt_desligamento,
  coalesce(fl_equipeminima, 0) as fl_equipeminima,
  coalesce(vinculo_equipe_ativo, 0) as vinculo_equipe_ativo,

  cast(null as int64) as inconsistente,

  dt_atualiza,

  tipo_enriquecimento_vinculo_unidade,
  chave_profissional_equipe_cbo,
  chave_profissional_unidade_cbo,

  current_timestamp() as created_at,
  current_timestamp() as updated_at

from equipes_profissionais
