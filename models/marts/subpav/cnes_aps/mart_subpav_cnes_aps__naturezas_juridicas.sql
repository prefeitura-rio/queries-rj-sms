{{
    config(
        materialized = 'table',
        alias        = "naturezas_juridicas",
        tags         = ["subpav", "cnes_aps"],
        cluster_by   = ["id"]
    )
}}

with base as (
    select
        safe_cast(nullif(CO_NATUREZA_JUR, '') as int64) as id,
        nullif(DS_NATUREZA_JUR, '') as descricao,
        _loaded_at as loaded_at,
        current_timestamp() as created_at,
        current_timestamp() as updated_at
    from {{ ref("raw_gdb_cnes__nfces085") }}
    where (
        data_particao = (
            select max(data_particao)
            from {{ ref("raw_gdb_cnes__nfces085") }}
        )
        and nullif(CO_NATUREZA_JUR, '') is not null
    )
    qualify row_number() over (
        partition by id
        order by loaded_at desc
    ) = 1
)

select *
from base
