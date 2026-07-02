{{
    config(
        materialized = 'table',
        alias        = "unidades_tipos",
        tags         = ["subpav", "cnes_aps"],
        cluster_by   = ["id"]
    )
}}

select
  safe_cast(nullif(TP_UNID_ID, '') as int64) as id,
  nullif(DESCRICAO, '') as descricao,
  current_timestamp() as created_at,
  current_timestamp() as updated_at
from {{ ref("raw_gdb_cnes__nfces010") }}
where nullif(TP_UNID_ID, '') is not null

qualify row_number() over (
  partition by safe_cast(nullif(TP_UNID_ID, '') as int64)
  order by data_particao desc, _loaded_at desc
) = 1
