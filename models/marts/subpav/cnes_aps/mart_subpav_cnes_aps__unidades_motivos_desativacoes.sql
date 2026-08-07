{{
    config(
        materialized = 'table',
        alias        = "unidades_motivos_desativacoes",
        tags         = ["subpav", "cnes_aps"],
        cluster_by   = ["id"]
    )
}}

select
  safe_cast(nullif(CD_MOTIVO_DESAB, '') as int64) as id,
  nullif(DS_MOTIVO_DESAB, '') as descricao,
  current_timestamp() as created_at,
  current_timestamp() as updated_at
from {{ ref("raw_gdb_cnes__nfces049") }}
where nullif(CD_MOTIVO_DESAB, '') is not null

qualify row_number() over (
  partition by safe_cast(nullif(CD_MOTIVO_DESAB, '') as int64)
  order by data_particao desc, _loaded_at desc
) = 1
