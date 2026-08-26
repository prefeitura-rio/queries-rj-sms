{{
    config(
        materialized = 'table',
        alias        = "unidades_tipos",
        tags         = ["subpav", "cnes_aps"],
        cluster_by   = ["id"]
    )
}}

with base as (
    select
        safe_cast(nullif(TP_UNID_ID, '') as int64) as id,
        nullif(DESCRICAO, '') as descricao,
        _loaded_at as loaded_at,
        current_timestamp() as created_at,
        current_timestamp() as updated_at
    from {{ ref("raw_gdb_cnes__nfces010") }}
    where (
        data_particao = (
            select max(data_particao)
            from {{ ref("raw_gdb_cnes__nfces010") }}
        )
        and nullif(TP_UNID_ID, '') is not null
    )
    qualify row_number() over (
        partition by id
        order by loaded_at desc
    ) = 1
)

select *
from base
