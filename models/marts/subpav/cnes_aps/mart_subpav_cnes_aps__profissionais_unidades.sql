{{
    config(
        materialized = 'table',
        alias        = "profissionais_unidades",
        tags         = ["subpav", "cnes_aps"],
        partition_by = {
            "field": "data_particao",
            "data_type": "date"
        },
        cluster_by   = ["cnes", "cpf", "cod_cbo", "competencia"]
    )
}}

with base as (
  select
    pu.*,
    c.competencia_id,
    c.competencia,
    lpad(cast(pu.cpf as string), 11, '0') as cpf_norm,
    lpad(cast(pu.cnes as string), 7, '0') as cnes_norm,
    upper(cast(pu.cod_cbo as string)) as cod_cbo_norm
  from {{ ref('int_subpav_cnes_aps__profissionais_unidades') }} pu
  inner join {{ ref('int_subpav_cnes_aps__competencias_legado') }} c
    on c.data_particao = pu.data_particao
  where pu.cpf is not null
    and pu.cnes is not null
    and pu.cod_cbo is not null
),

profissionais_unidades as (
  select *
  from base

  qualify row_number() over (
    partition by
      competencia_id,
      cpf_norm,
      cnes_norm,
      cod_cbo_norm
    order by
      loaded_at desc,
      dt_atualiza desc,
      carga_horaria_total desc,
      profissional_id_original desc,
      _source_file desc
  ) = 1
)

select
  data_particao,
  competencia,
  competencia_id,

  cast(null as int64) as id,
  cast(null as int64) as unidade_id,
  cast(null as int64) as profissional_id,
  cast(null as int64) as cbo_id,

  cpf_norm as cpf,
  cnes_norm as cnes,
  cod_cbo_norm as cod_cbo,

  coalesce(cg_horaamb, 0) as cg_horaamb,
  coalesce(cg_horahosp, 0) as cg_horahosp,
  coalesce(cg_horaoutr, 0) as cg_horaoutr,

  coalesce(numero_registro, '') as n_registro,
  coalesce(uf_registro, '') as sg_uf_crm,

  coalesce(tp_preceptor, 0) as tp_preceptor,
  coalesce(tp_residente, 0) as tp_residente,

  dt_atualiza,

  safe_cast(vinculacao_id_original as int64) as vinculacao_id,
  safe_cast(conselho_id_original as int64) as conselho_id,

  tipo_sus_nao_sus,
  detalhe_terceirizado_sih,
  cnpj_detalhe_vinculo,

  chave_profissional_unidade_cbo,

  current_timestamp() as created_at,
  current_timestamp() as updated_at

from profissionais_unidades
