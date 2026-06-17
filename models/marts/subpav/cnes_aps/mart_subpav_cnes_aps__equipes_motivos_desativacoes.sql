{{
    config(
        materialized = 'table',
        alias        = "equipes_motivos_desativacoes",
        tags         = ["subpav", "cnes_aps"],
        cluster_by   = ["id"]
    )
}}

select
  safe_cast(nullif(CD_MOTIVO_DESATIV, '') as int64) as id,
  nullif(DS_MOTIVO_DESATIV, '') as descricao,
  current_timestamp() as created_at,
  current_timestamp() as updated_at
from {{ ref("raw_gdb_cnes__nfces053") }}
where nullif(CD_MOTIVO_DESATIV, '') is not null

qualify row_number() over (
  partition by safe_cast(nullif(CD_MOTIVO_DESATIV, '') as int64)
  order by data_particao desc, _loaded_at desc
) = 1
