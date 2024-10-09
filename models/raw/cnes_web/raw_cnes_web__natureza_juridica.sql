{{
    config(
        alias="natureza_juridica",
    )
}}

with source as (select * from {{ source("brutos_cnes_web_staging", "tbNaturezaJuridica") }})

select 
    -- pk
    safe_cast(CO_NATUREZA_JUR as string) as id_natureza_juridica,

    -- common
    safe_cast(DS_NATUREZA_JUR as string) as descricao,

    -- metadata
    safe_cast(mes_particao as string) as mes_particao,
    safe_cast(ano_particao as string) as ano_particao,
    concat(
        safe_cast(ano_particao as string),
        '-',
        safe_cast(mes_particao as string),
        '-01'
    ) as data_particao,
    safe_cast(_data_carga as datetime) as data_carga,
    safe_cast(_data_snapshot as datetime) as data_snapshot

from source