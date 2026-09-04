{{
    config(
        alias="fat_diagnostico",
        materialized="incremental",
        unique_key="diagnostico_gid"
    )
}}

with
source as (
    select *
    from {{ source("brutos_prontuario_vitai_dtw_staging", "fat_diagnostico") }}
),

renomeado as (
    select
        safe_cast(data_diagnostico as datetime) as data_diagnostico,
        safe_cast(updated_at as datetime) as updated_at,
        safe_cast(diagnostico_gid as string) as diagnostico_gid,
        safe_cast(tpd_id as int64) as tpd_id,
        safe_cast(paciente_gid as string) as paciente_gid,
        safe_cast(fat_paciente_rede_id as int64) as fat_paciente_rede_id,
        safe_cast(cid_id as int64) as cid_id,
        safe_cast(boletim_gid as string) as boletim_gid,
        safe_cast(estabelecimento_gid as string) as estabelecimento_gid,
        safe_cast(created_at as datetime) as created_at,

        -- Metadados
        datetime(timestamp(datalake_loaded_at), 'America/Sao_Paulo') as loaded_at,
        safe_cast(ano_particao as int64) as ano_particao,
        safe_cast(mes_particao as int64) as mes_particao,
        safe_cast(data_particao as date) as data_particao
    from source
)

select * from renomeado
