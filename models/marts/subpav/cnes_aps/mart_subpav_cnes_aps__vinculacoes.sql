{{
    config(
        materialized = 'table',
        alias        = "vinculacoes",
        tags         = ["subpav", "cnes_aps"],
        cluster_by   = ["id", "vinculacao_unidade_id", "vinculacao_empregador_id"]
    )
}}

with base as (
  select *
  from {{ ref("raw_gdb_cnes__nfces058") }}
  where nullif(IND_VINC, '') is not null

  qualify row_number() over (
    partition by safe_cast(nullif(IND_VINC, '') as int64)
    order by data_particao desc, _loaded_at desc, _source_file desc
  ) = 1
)

select
  safe_cast(nullif(IND_VINC, '') as int64) as id,
  safe_cast(nullif(TP_SUBVINCULO, '') as int64) as tp_subvinculo,
  nullif(DS_SUBVINCULO, '') as ds_subvinculo,
  coalesce(nullif(ST_HABILITADO, ''), '') as st_habilitado,
  safe_cast(nullif(CD_VINCULACAO, '') as int64) as vinculacao_unidade_id,
  safe_cast(nullif(TP_VINCULO, '') as int64) as vinculacao_empregador_id,
  current_timestamp() as created_at,
  current_timestamp() as updated_at
from base
