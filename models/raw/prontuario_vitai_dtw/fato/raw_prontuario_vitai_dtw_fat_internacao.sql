{{
    config(
        alias="fat_internacao",
        materialized="incremental",
        unique_key="internacao_gid"
    )
}}

with
source as (
    select *
    from {{ source("brutos_prontuario_vitai_dtw_staging", "fat_internacao") }}
),

renomeado as (
    select
        safe_cast(created_at as datetime) as created_at,
        safe_cast(fat_paciente_rede_id as int64) as fat_paciente_rede_id,
        safe_cast(paciente_gid as string) as paciente_gid,
        safe_cast(data_saida as datetime) as data_saida,
        safe_cast(esp_id as int64) as esp_id,
        safe_cast(tin_id as int64) as tin_id,
        safe_cast(cid_id_internacao as int64) as cid_id_internacao,
        safe_cast(data_entrada as datetime) as data_entrada,
        safe_cast(estabelecimento_gid as string) as estabelecimento_gid,
        safe_cast(prc_id as int64) as prc_id,
        safe_cast(boletim_gid as string) as boletim_gid,
        safe_cast(prf_id as int64) as prf_id,
        safe_cast(internacao_gid as string) as internacao_gid,
        safe_cast(updated_at as datetime) as updated_at,

        -- Metadados
        datetime(timestamp(datalake_loaded_at), 'America/Sao_Paulo') as loaded_at,
        safe_cast(ano_particao as int64) as ano_particao,
        safe_cast(mes_particao as int64) as mes_particao,
        safe_cast(data_particao as date) as data_particao
    from source
)

select * from renomeado
