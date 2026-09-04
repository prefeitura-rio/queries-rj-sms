{{
    config(
        alias="fat_boletim",
        materialized="incremental",
        unique_key="boletim_gid"
    )
}}

with
source as (
    select *
    from {{ source("brutos_prontuario_vitai_dtw_staging", "fat_boletim") }}
),

renomeado as (
    select
        safe_cast(tpu_id as int64) as tpu_id,
        safe_cast(data_internacao as datetime) as data_internacao,
        safe_cast(idade as int64) as idade,
        safe_cast(ris_id as int64) as ris_id,
        safe_cast(interno as string) as interno,
        safe_cast(boletim_gid as string) as boletim_gid,
        safe_cast(dea_id as int64) as dea_id,
        safe_cast(created_at as datetime) as created_at,
        safe_cast(fae_id as int64) as fae_id,
        safe_cast(estabelecimento_gid as string) as estabelecimento_gid,
        safe_cast(mob_id as int64) as mob_id,
        safe_cast(pri_id as int64) as pri_id,
        safe_cast(numero_be as int64) as numero_be,
        safe_cast(data_alta as datetime) as data_alta,
        safe_cast(orp_id as int64) as orp_id,
        safe_cast(tpe_id as int64) as tpe_id,
        safe_cast(updated_at as datetime) as updated_at,
        safe_cast(esp_id as int64) as esp_id,
        safe_cast(cbo_id as int64) as cbo_id,
        safe_cast(fat_paciente_rede_id as int64) as fat_paciente_rede_id,
        safe_cast(tpa_id as int64) as tpa_id,
        safe_cast(datahora as datetime) as datahora,
        safe_cast(met_id as int64) as met_id,
        safe_cast(data_entrada as datetime) as data_entrada,
        safe_cast(paciente_gid as string) as paciente_gid,

        -- Metadados
        datetime(timestamp(datalake_loaded_at), 'America/Sao_Paulo') as loaded_at,
        safe_cast(ano_particao as int64) as ano_particao,
        safe_cast(mes_particao as int64) as mes_particao,
        safe_cast(data_particao as date) as data_particao
    from source
)

select * from renomeado
