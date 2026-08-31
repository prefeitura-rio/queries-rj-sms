{{
    config(
        materialized = 'table',
        alias        = "profissionais",
        tags         = ["subpav", "cnes_aps"],
        cluster_by   = ["cpf", "cns"]
    )
}}

with profissionais as (
  select
    *
  from {{ ref('int_subpav_cnes_aps__profissionais') }}
  where cpf is not null

  qualify row_number() over (
    partition by cpf
    order by
      data_particao desc,
      dt_atualiza desc,
      loaded_at desc,
      _source_file desc
  ) = 1
)

select
  data_particao as data_particao_origem,

  cast(null as int64) as id,

  lpad(cast(cpf as string), 11, '0') as cpf,
  lpad(cast(cns as string), 15, '0') as cns,
  nome_profissional as nome,
  dt_nascimento as dt_nasc,

  telefone,
  email,

  nome_pais_origem as nome_pais,
  safe_cast(nacionalidade_indicador_original as int64) as ind_nacio,

  dt_atualiza,

  safe_cast(sexo_id as int64) as sexo_id,
  safe_cast(raca_cor_id as int64) as raca_cor_id,
  safe_cast(nivel_escolaridade_id as int64) as nivel_escolaridade_id,

  current_timestamp() as created_at,
  current_timestamp() as updated_at

from profissionais
