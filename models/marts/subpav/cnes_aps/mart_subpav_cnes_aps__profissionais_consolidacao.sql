{{
    config(
        materialized = 'table',
        alias        = "profissionais_consolidacao",
        tags         = ["subpav", "cnes_aps"],
        partition_by = {
            "field": "data_particao",
            "data_type": "date"
        },
        cluster_by = ["competencia", "cnes", "cpf", "cod_cbo"]
    )
}}

with profissionais_consolidacao as (
  select
    data_particao,
    competencia,
    competencia_id,

    cpf,
    cns,
    nome_profissional,
    profissional_id_original,

    cnes,
    unidade_id_original,
    nome_unidade,

    cod_cbo,

    ine,
    nm_referencia,
    tipo_equipe_id,
    tipo_equipe_descricao,

    sexo_id,
    sexo,

    cg_horaamb,
    cg_horahosp,
    cg_horaoutr,
    carga_horaria_total,

    dt_entrada,
    dt_desligamento,
    possui_dt_desligamento,
    dias_desligado,

    fl_equipeminima,
    inconsistente,

    dt_atualiza,
    dt_atualiza_profissional,
    dt_atualiza_unidade,
    dt_atualiza_equipe,

    possui_vinculo_equipe,
    vinculo_equipe_ativo,
    equipe_ativa,

    chave_profissional_unidade_cbo_base,
    chave_profissional_equipe_cbo

  from {{ ref('int_subpav_cnes_aps__profissionais_consolidacao') }}
)

select
  data_particao,
  competencia,
  competencia_id,

  lpad(cast(cpf as string), 11, '0') as cpf,
  cns,
  nome_profissional,
  profissional_id_original,

  lpad(cast(cnes as string), 7, '0') as cnes,
  unidade_id_original,
  nome_unidade,

  cod_cbo,

  lpad(cast(ine as string), 10, '0') as ine,
  nm_referencia,
  tipo_equipe_id,
  tipo_equipe_descricao,

  sexo_id,
  sexo,

  cast(null as int64) as profissional_id,
  cast(null as int64) as unidade_id,
  cast(null as int64) as cbo_id,
  cast(null as int64) as equipe_id,

  coalesce(cg_horaamb, 0) as cg_horaamb,
  coalesce(cg_horahosp, 0) as cg_horahosp,
  coalesce(cg_horaoutr, 0) as cg_horaoutr,
  coalesce(carga_horaria_total, 0) as carga_horaria_total,

  dt_entrada,
  dt_desligamento,
  coalesce(possui_dt_desligamento, 0) as possui_dt_desligamento,
  dias_desligado,

  coalesce(fl_equipeminima, 0) as fl_equipeminima,
  coalesce(inconsistente, 0) as inconsistente,

  dt_atualiza,
  dt_atualiza_profissional,
  dt_atualiza_unidade,
  dt_atualiza_equipe,

  coalesce(possui_vinculo_equipe, 0) as possui_vinculo_equipe,
  coalesce(vinculo_equipe_ativo, 0) as vinculo_equipe_ativo,
  coalesce(equipe_ativa, 0) as equipe_ativa,

  chave_profissional_unidade_cbo_base,
  chave_profissional_equipe_cbo,

  current_timestamp() as created_at,
  current_timestamp() as updated_at

from profissionais_consolidacao
