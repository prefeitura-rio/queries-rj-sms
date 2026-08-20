{{
    config(
        materialized = 'table',
        alias        = "unidades",
        tags         = ["subpav", "cnes_aps"],
        cluster_by   = ["cnes", "ap", "tipo_unidade_id"]
    )
}}

with unidades_base as (
  select
    *,
    lpad(cast(cnes as string), 7, '0') as cnes_norm
  from {{ ref('int_subpav_cnes_aps__unidades') }}
  where cnes is not null
),

unidades as (
  select *
  from unidades_base

  qualify row_number() over (
    partition by cnes_norm
    order by
      data_particao desc,
      dt_atualiza desc,
      _loaded_at desc,
      _source_file desc
  ) = 1
)

select
  data_particao as data_particao_origem,

  cast(null as int64) as id,

  cnes_norm as cnes,
  safe_cast(ap as int64) as ap,
  coalesce(
    nullif(trim(nome_fanta), ''),
    nullif(trim(razao_social), ''),
    'NAO INFORMADO'
  ) as nome_fanta,
  razao_social as r_social,
  dt_atualiza,

  coalesce(sigla_gestao, tipo_gestao_cnes) as tp_gestao,
  coalesce(safe_cast(tipo_estab_sempre_aberto as int64), 0) as tp_estab_sempre_aberto,

  cast(null as date) as dt_inaugura,

  safe_cast(tipo_unidade_id as int64) as tipo_unidade_id,
  safe_cast(turno_atendimento_id as int64) as cod_turnat_id,
  safe_cast(motivo_desativacao_unidade_id as int64) as motivo_desativacao_unidade_id,
  safe_cast(natureza_juridica_id as int64) as natureza_juridica_id,
  safe_cast(tipo_estabelecimento_id as int64) as tipo_estabelecimento_id,
  safe_cast(atividade_principal_id as int64) as atividade_principal_id,

  cast(null as int64) as prof_diretor_id,
  lpad(cast(cpf_diretor_clinico as string), 11, '0') as cpf_diretor_clinico,

  tipo_unidade_sms,
  is_unidade_aps_panorama,
  unidade_ativa,
  status,
  status_movimento,

  current_timestamp() as created_at,
  current_timestamp() as updated_at

from unidades
