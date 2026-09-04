{{
    config(
        alias="fat_classificacao",
        materialized="incremental",
        unique_key="classificacao_gid"
    )
}}

with
source as (
    select *
    from {{ source("brutos_prontuario_vitai_dtw_staging", "fat_classificacao") }}
),

renomeado as (
    select
        safe_cast(data_fim as datetime) as data_fim,
        safe_cast(meta as int64) as meta,
        safe_cast(descritor as string) as descritor,
        safe_cast(primeira as string) as primeira,
        safe_cast(asv_queixa_principal as string) as asv_queixa_principal,
        safe_cast(prf_id as int64) as prf_id,
        safe_cast(created_at as datetime) as created_at,
        safe_cast(classificacao_gid as string) as classificacao_gid,
        safe_cast(estabelecimento_gid as string) as estabelecimento_gid,
        safe_cast(data_inicio as datetime) as data_inicio,
        safe_cast(ultima as string) as ultima,
        safe_cast(fat_paciente_rede_id as int64) as fat_paciente_rede_id,
        safe_cast(paciente_gid as string) as paciente_gid,
        safe_cast(ris_id as int64) as ris_id,
        safe_cast(updated_at as datetime) as updated_at,
        safe_cast(boletim_gid as string) as boletim_gid,

        -- Metadados
        datetime(timestamp(datalake_loaded_at), 'America/Sao_Paulo') as loaded_at,
        safe_cast(ano_particao as int64) as ano_particao,
        safe_cast(mes_particao as int64) as mes_particao,
        safe_cast(data_particao as date) as data_particao
    from source
)

select * from renomeado
