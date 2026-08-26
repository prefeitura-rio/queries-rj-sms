{{
    config(
        materialized = 'table',
        alias        = "equipes_motivos_desativacoes",
        tags         = ["subpav", "cnes_aps"],
        cluster_by   = ["id"]
    )
}}

with base as (
    select
        safe_cast(nullif(CD_MOTIVO_DESATIV, '') as int64) as id,
        nullif(DS_MOTIVO_DESATIV, '') as descricao,

        _loaded_at as loaded_at,
        current_timestamp() as created_at,
        current_timestamp() as updated_at
    from {{ ref("raw_gdb_cnes__nfces053") }}
    where (
        data_particao = (
            select max(data_particao)
            from {{ ref("raw_gdb_cnes__nfces053") }}
        )
        and nullif(CD_MOTIVO_DESATIV, '') is not null
    )
    qualify row_number() over (
        partition by id
        order by loaded_at desc
    ) = 1
)

select *
from base
