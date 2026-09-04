{{
    config(
        alias="fat_atendimento",
        materialized="incremental",
        unique_key="atendimento_gid"
    )
}}

with
source as (
    select *
    from {{ source("brutos_prontuario_vitai_dtw_staging", "fat_atendimento") }}
),

renomeado as (
    select
        safe_cast(queixa as string) as queixa,
        safe_cast(created_at as datetime) as created_at,
        safe_cast(matd_id as int64) as matd_id,
        safe_cast(paciente_gid as string) as paciente_gid,
        safe_cast(data_fim as datetime) as data_fim,
        safe_cast(cid_id as int64) as cid_id,
        safe_cast(boletim_gid as string) as boletim_gid,
        safe_cast(data_inicio as datetime) as data_inicio,
        safe_cast(updated_at as datetime) as updated_at,
        safe_cast(estabelecimento_gid as string) as estabelecimento_gid,
        safe_cast(primeiro_atendimento as string) as primeiro_atendimento,
        safe_cast(fat_paciente_rede_id as int64) as fat_paciente_rede_id,
        safe_cast(atendimento_gid as string) as atendimento_gid,
        safe_cast(esp_id as int64) as esp_id,
        safe_cast(prf_id as int64) as prf_id,
        safe_cast(ultimo_atendimento as string) as ultimo_atendimento,

        -- Metadados
        datetime(timestamp(datalake_loaded_at), 'America/Sao_Paulo') as loaded_at,
        safe_cast(ano_particao as int64) as ano_particao,
        safe_cast(mes_particao as int64) as mes_particao,
        safe_cast(data_particao as date) as data_particao
    from source
)

select * from renomeado
