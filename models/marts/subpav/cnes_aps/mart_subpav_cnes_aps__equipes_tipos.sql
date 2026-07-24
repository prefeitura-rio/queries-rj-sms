{{
    config(
        materialized = 'table',
        alias        = "equipes_tipos",
        tags         = ["subpav", "cnes_aps"],
        cluster_by   = ["id"]
    )
}}

with grupos as (
  select
    nullif(CO_GRUPO_EQUIPE, '') as co_grupo_equipe,
    nullif(NO_GRUPO_EQUIPE, '') as no_grupo_equipe
  from {{ ref("raw_gdb_cnes__nfces090") }}
  where nullif(CO_GRUPO_EQUIPE, '') is not null

  qualify row_number() over (
    partition by nullif(CO_GRUPO_EQUIPE, '')
    order by data_particao desc, _loaded_at desc, _source_file desc
  ) = 1
),

tipos as (
  select
    *
  from {{ ref("raw_gdb_cnes__nfces046") }}
  where nullif(TP_EQUIPE, '') is not null

  qualify row_number() over (
    partition by safe_cast(nullif(TP_EQUIPE, '') as int64)
    order by data_particao desc, _loaded_at desc, _source_file desc
  ) = 1
)

select
  safe_cast(nullif(t.TP_EQUIPE, '') as int64) as id,
  nullif(t.DS_EQUIPE, '') as ds_tipo_equipe,
  coalesce(g.no_grupo_equipe, '') as grupo,
  current_timestamp() as created_at,
  current_timestamp() as updated_at
from tipos t
left join grupos g
  on g.co_grupo_equipe = nullif(t.CO_GRUPO_EQUIPE, '')
