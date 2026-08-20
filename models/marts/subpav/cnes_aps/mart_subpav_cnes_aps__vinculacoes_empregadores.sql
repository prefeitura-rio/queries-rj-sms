{{
    config(
        materialized = 'table',
        alias        = "vinculacoes_empregadores",
        tags         = ["subpav", "cnes_aps"],
        cluster_by   = ["id"]
    )
}}

with base as (
  select *
  from {{ ref("raw_gdb_cnes__nfces057") }}
  where nullif(TP_VINCULO, '') is not null

  qualify row_number() over (
    partition by safe_cast(nullif(TP_VINCULO, '') as int64)
    order by data_particao desc, _loaded_at desc, _source_file desc
  ) = 1
)

select
  safe_cast(nullif(TP_VINCULO, '') as int64) as id,
  nullif(DS_VINCULO, '') as ds_vinculacao_empregador,
  current_timestamp() as created_at,
  current_timestamp() as updated_at
from base
