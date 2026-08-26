{{
    config(
        materialized = 'table',
        alias        = "vinculacoes_unidades",
        tags         = ["subpav", "cnes_aps"],
        cluster_by   = ["id"]
    )
}}

with vinculacoes_unidades_gdb as (
    select
        safe_cast(nullif(CD_VINCULACAO, '') as int64) as id,
        nullif(DS_VINCULACAO, '') as ds_vinculacao_unidade,
        _loaded_at as loaded_at,
        current_timestamp() as created_at,
        current_timestamp() as updated_at
    from {{ ref("raw_gdb_cnes__nfces056") }}
    where (
        data_particao = (
            select max(data_particao)
            from {{ ref("raw_gdb_cnes__nfces056") }}
        )
        and nullif(CD_VINCULACAO, '') is not null
    )
    qualify row_number() over (
        partition by id
        order by loaded_at desc
    ) = 1
),

vinculacoes_unidades_complementares as (
    select
        0 as id,
        'A CONFIRMAR' as ds_vinculacao_unidade,
        current_timestamp() as loaded_at,
        current_timestamp() as created_at,
        current_timestamp() as updated_at
    from (select 1)
    where not exists (
        select 1
        from vinculacoes_unidades_gdb
        where id = 0
    )
)

select *
from vinculacoes_unidades_gdb

union all

select *
from vinculacoes_unidades_complementares
