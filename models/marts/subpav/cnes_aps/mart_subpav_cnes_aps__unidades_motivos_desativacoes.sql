{{
    config(
        materialized = 'table',
        alias        = "unidades_motivos_desativacoes",
        tags         = ["subpav", "cnes_aps"],
        cluster_by   = ["id"]
    )
}}

with base as (
    select
        safe_cast(nullif(CD_MOTIVO_DESAB, '') as int64) as id,
        nullif(DS_MOTIVO_DESAB, '') as descricao,
        _loaded_at as loaded_at,
        current_timestamp() as created_at,
        current_timestamp() as updated_at
    from {{ ref("raw_gdb_cnes__nfces049") }}
    where (
        data_particao = (
            select max(data_particao)
            from {{ ref("raw_gdb_cnes__nfces049") }}
        )
        and nullif(CD_MOTIVO_DESAB, '') is not null
    )
    qualify row_number() over (
        partition by id
        order by loaded_at desc
    ) = 1
)

select *
from base
