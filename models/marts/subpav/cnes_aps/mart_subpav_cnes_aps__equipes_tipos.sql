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
        NO_GRUPO_EQUIPE as no_grupo_equipe,
        _loaded_at as loaded_at
    from {{ ref("raw_gdb_cnes__nfces090") }}
    where (
        data_particao = (
            select max(data_particao)
            from {{ ref("raw_gdb_cnes__nfces090") }}
        )
        and nullif(CO_GRUPO_EQUIPE, '') is not null
    )
    qualify row_number() over (
        partition by co_grupo_equipe
        order by loaded_at desc
    ) = 1
),

tipos as (
    select
        safe_cast(nullif(TP_EQUIPE, '') as int64) as id,
        nullif(DS_EQUIPE, '') as ds_tipo_equipe,
        nullif(CO_GRUPO_EQUIPE, '') as co_grupo_equipe,
        _loaded_at as loaded_at
    from {{ ref("raw_gdb_cnes__nfces046") }}
    where (
        data_particao = (
            select max(data_particao)
            from {{ ref("raw_gdb_cnes__nfces046") }}
        )
        and nullif(TP_EQUIPE, '') is not null
    )
    qualify row_number() over (
        partition by id
        order by loaded_at desc
    ) = 1
)

select
    t.id,
    t.ds_tipo_equipe,
    g.no_grupo_equipe as grupo,
    coalesce(t.loaded_at, g.loaded_at) as loaded_at,
    current_timestamp() as created_at,
    current_timestamp() as updated_at
from tipos t
left join grupos g
    using(co_grupo_equipe)
