{{
    config(
        materialized = 'table',
        alias        = "vinculacoes_empregadores",
        tags         = ["subpav", "cnes_aps"],
        cluster_by   = ["id"]
    )
}}

with base as (
    select
        safe_cast(nullif(TP_VINCULO, '') as int64) as id,
        nullif(DS_VINCULO, '') as ds_vinculacao_empregador,
        _loaded_at as loaded_at,
        current_timestamp() as created_at,
        current_timestamp() as updated_at
    from {{ ref("raw_gdb_cnes__nfces057") }}
    where (
        data_particao = (
            select max(data_particao)
            from {{ ref("raw_gdb_cnes__nfces057") }}
        )
        and nullif(TP_VINCULO, '') is not null
    )
    qualify row_number() over (
        partition by id
        order by loaded_at desc
    ) = 1
)

select *
from base
