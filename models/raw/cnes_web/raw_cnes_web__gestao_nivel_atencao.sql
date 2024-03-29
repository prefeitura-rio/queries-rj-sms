{{
    config(
        alias="gestao_nivel_atencao",
    )
}}

with source as (select * from {{ source("brutos_cnes_web_staging", "rlEstabProgFundo") }})
select
    -- Primary Key
    -- Foreign Keys
    safe_cast(co_unidade as string) as id_unidade,
    safe_cast(co_atividade as string) as id_nivel_atencao,

    -- Common fields
    safe_cast(tp_estadual_municipal as string) as gestao_estadual_municipal,

    -- Metadata
    safe_cast(
        dt_atualizacao_origem as date format 'DD/MM/YYYY'
    ) as data_entrada_sistema,
    safe_cast(dt_atualizacao as date format 'DD/MM/YYYY') as data_atualizao_registro,
    safe_cast(co_usuario as string) as usuario_atualizador_registro,
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
