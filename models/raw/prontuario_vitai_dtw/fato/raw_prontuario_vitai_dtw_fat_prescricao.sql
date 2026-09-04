{{
    config(
        alias="fat_prescricao",
        materialized="incremental",
        unique_key="prescricao_gid"
    )
}}

with
source as (
    select *
    from {{ source("brutos_prontuario_vitai_dtw_staging", "fat_prescricao") }}
),

renomeado as (
    select
        safe_cast(boletim_gid as string) as boletim_gid,
        safe_cast(prescricao_gid as string) as prescricao_gid,
        safe_cast(prf_id as int64) as prf_id,
        safe_cast(data_prescricao as datetime) as data_prescricao,
        safe_cast(paciente_gid as string) as paciente_gid,
        safe_cast(pre_rotinaenfermagem as string) as pre_rotinaenfermagem,
        safe_cast(created_at as datetime) as created_at,
        safe_cast(updated_at as datetime) as updated_at,
        safe_cast(tpr_id as int64) as tpr_id,
        safe_cast(fat_paciente_rede_id as int64) as fat_paciente_rede_id,
        safe_cast(estabelecimento_gid as string) as estabelecimento_gid,
        safe_cast(pre_urgencia as string) as pre_urgencia,

        -- Metadados
        datetime(timestamp(datalake_loaded_at), 'America/Sao_Paulo') as loaded_at,
        safe_cast(ano_particao as int64) as ano_particao,
        safe_cast(mes_particao as int64) as mes_particao,
        safe_cast(data_particao as date) as data_particao
    from source
)

select * from renomeado
