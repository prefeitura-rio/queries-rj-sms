{{
    config(
        alias="fat_paciente_regulado",
        materialized="incremental",
        unique_key="paciente_regulado_gid"
    )
}}

with
source as (
    select *
    from {{ source("brutos_prontuario_vitai_dtw_staging", "fat_paciente_regulado") }}
),

renomeado as (
    select
        safe_cast(unidade_destino as string) as unidade_destino,
        safe_cast(paciente_gid as string) as paciente_gid,
        safe_cast(prf_id as int64) as prf_id,
        safe_cast(data_regulacao as string) as data_regulacao,
        safe_cast(data_exclusao as string) as data_exclusao,
        safe_cast(paciente_regulado_gid as string) as paciente_regulado_gid,
        safe_cast(estabelecimento_gid as string) as estabelecimento_gid,
        safe_cast(fat_paciente_rede_id as int64) as fat_paciente_rede_id,
        safe_cast(boletim_gid as string) as boletim_gid,
        safe_cast(data_cadastro as datetime) as data_cadastro,
        safe_cast(created_at as datetime) as created_at,
        safe_cast(sec_id as int64) as sec_id,
        safe_cast(protocolo_regulacao as string) as protocolo_regulacao,
        safe_cast(sistema_regulacao as string) as sistema_regulacao,
        safe_cast(updated_at as datetime) as updated_at,
        safe_cast(esp_id as int64) as esp_id,

        -- Metadados
        datetime(timestamp(datalake_loaded_at), 'America/Sao_Paulo') as loaded_at,
        safe_cast(ano_particao as int64) as ano_particao,
        safe_cast(mes_particao as int64) as mes_particao,
        safe_cast(data_particao as date) as data_particao
    from source
)

select * from renomeado
