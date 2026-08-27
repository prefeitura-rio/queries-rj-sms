{{
    config(
        materialized = 'table',
        alias        = "cbos",
        tags         = ["subpav", "cnes_aps"],
        cluster_by   = ["cod_cbo"]
    )
}}

with cbos as (
    select
        cast(null as int64) as id,
        nullif(COD_CBO, '') as cod_cbo,
        nullif(DESCRICAO, '') as ds_cbo,
        coalesce(safe_cast(nullif(TP_CBO_SAUDE, '') as int64), 0) as tp_cbo_saude,
        0 as nivel_superior,
        0 as nivel_medio,
        _loaded_at as loaded_at,
        current_timestamp() as created_at,
        current_timestamp() as updated_at

    from {{ ref("raw_gdb_cnes__nfces026") }}
    where (
        data_particao = (
            select max(data_particao)
            from {{ ref("raw_gdb_cnes__nfces026") }}
        )
        and nullif(COD_CBO, '') is not null
    )
    qualify row_number() over (
        partition by cod_cbo
        order by loaded_at desc
    ) = 1
)

select *
from cbos
