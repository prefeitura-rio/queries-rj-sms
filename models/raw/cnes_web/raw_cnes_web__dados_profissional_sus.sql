{{
    config(
        alias="dados_profissionais_sus",
    )
}}

with
    source as (
        select * from {{ source("brutos_cnes_web_staging", "tbDadosProfissionalSus") }}
    )
select
    safe_cast(co_profissional_sus as string) as codigo_sus,
    safe_cast(co_cns as string) as cns,
    safe_cast(no_profissional as string) as nome,
    safe_cast(dt_atualizacao as date format 'DD/MM/YYYY') as data_atualizacao,
    safe_cast(co_usuario as string) as usuario_atualizador_registro,
    safe_cast(co_nacionalidade as string) as codigo_nacionalidade,
    safe_cast(co_seq_inclusao as string) as codigo_sequencial_inclusao,
    safe_cast(
        dt_atualizacao_origem as date format 'DD/MM/YYYY'
    ) as data_atualizacao_origem,
    safe_cast(mes_particao as string) as mes_particao,
    safe_cast(ano_particao as string) as ano_particao,
    concat(
        safe_cast(ano_particao as string), '-', safe_cast(mes_particao as string), '-01'
    ) as data_particao,
    safe_cast(_data_carga as datetime) as data_carga,
    safe_cast(_data_snapshot as datetime) as data_snapshot

from source
