{{
    config(
        alias="fat_alergia",
        materialized="incremental",
        unique_key="alergia_gid"
    )
}}

with
source as (
    select *
    from {{ source("brutos_prontuario_vitai_dtw_staging", "fat_alergia") }}
),

renomeado as (
    select
        safe_cast(fat_paciente_rede_id as int64) as fat_paciente_rede_id,
        safe_cast(data_especifica as date) as data_especifica,
        safe_cast(boletim_gid as string) as boletim_gid,
        safe_cast(ativo as string) as ativo,
        safe_cast(updated_at as datetime) as updated_at,
        safe_cast(tal_id as int64) as tal_id,
        safe_cast(data_registro as datetime) as data_registro,
        safe_cast(alergia_gid as string) as alergia_gid,
        safe_cast(reacao as string) as reacao,
        safe_cast(sev_id as int64) as sev_id,
        safe_cast(observacao as string) as observacao,
        safe_cast(paciente_gid as string) as paciente_gid,
        safe_cast(data_inclusao as datetime) as data_inclusao,
        safe_cast(created_at as datetime) as created_at,
        safe_cast(estabelecimento_gid as string) as estabelecimento_gid,

        -- Metadados
        datetime(timestamp(datalake_loaded_at), 'America/Sao_Paulo') as loaded_at,
        safe_cast(ano_particao as int64) as ano_particao,
        safe_cast(mes_particao as int64) as mes_particao,
        safe_cast(data_particao as date) as data_particao
    from source
)

select * from renomeado
