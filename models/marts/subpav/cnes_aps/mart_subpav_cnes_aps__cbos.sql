{{
    config(
        materialized = 'table',
        alias        = "cbos",
        tags         = ["subpav", "cnes_aps"],
        cluster_by   = ["cod_cbo"]
    )
}}

with cbos as (
  select *
  from {{ ref("raw_gdb_cnes__nfces026") }}
  where nullif(COD_CBO, '') is not null

  qualify row_number() over (
    partition by nullif(COD_CBO, '')
    order by data_particao desc, _loaded_at desc, _source_file desc
  ) = 1
)

select
  cast(null as int64) as id,
  nullif(COD_CBO, '') as cod_cbo,
  nullif(DESCRICAO, '') as ds_cbo,
  coalesce(safe_cast(nullif(TP_CBO_SAUDE, '') as int64), 0) as tp_cbo_saude,
  0 as nivel_superior,
  0 as nivel_medio,
  current_timestamp() as created_at,
  current_timestamp() as updated_at
from cbos
