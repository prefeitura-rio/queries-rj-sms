{{
    config(
        materialized = 'table',
        alias        = "naturezas_juridicas",
        tags         = ["subpav", "cnes_aps"],
        cluster_by   = ["id"]
    )
}}

select
  safe_cast(nullif(CO_NATUREZA_JUR, '') as int64) as id,
  nullif(DS_NATUREZA_JUR, '') as descricao,
  current_timestamp() as created_at,
  current_timestamp() as updated_at
from {{ ref("raw_gdb_cnes__nfces085") }}
where nullif(CO_NATUREZA_JUR, '') is not null

qualify row_number() over (
  partition by safe_cast(nullif(CO_NATUREZA_JUR, '') as int64)
  order by data_particao desc, _loaded_at desc
) = 1
