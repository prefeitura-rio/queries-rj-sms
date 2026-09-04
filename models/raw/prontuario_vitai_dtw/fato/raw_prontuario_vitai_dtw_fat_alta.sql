{{
    config(
        alias="fat_alta",
        materialized="incremental",
        unique_key="alta_gid"
    )
}}

with
source as (
    select *
    from {{ source("brutos_prontuario_vitai_dtw_staging", "fat_alta") }}
),

renomeado as (
    select
        safe_cast(tipo_alta_detalhada as string) as tipo_alta_detalhada,
        safe_cast(mal_id as int64) as mal_id,
        safe_cast(data_obito as datetime) as data_obito,
        safe_cast(boletim_gid as string) as boletim_gid,
        safe_cast(created_at as datetime) as created_at,
        safe_cast(alta_gid as string) as alta_gid,
        safe_cast(abe_obs as string) as abe_obs,
        safe_cast(alta_administrativa as datetime) as alta_administrativa,
        safe_cast(status as string) as status,
        safe_cast(estabelecimento_gid as string) as estabelecimento_gid,
        safe_cast(updated_at as datetime) as updated_at,
        safe_cast(data_alta as datetime) as data_alta,
        safe_cast(alta_medica as datetime) as alta_medica,
        safe_cast(motivo_saida as string) as motivo_saida,

        -- Metadados
        datetime(timestamp(datalake_loaded_at), 'America/Sao_Paulo') as loaded_at,
        safe_cast(ano_particao as int64) as ano_particao,
        safe_cast(mes_particao as int64) as mes_particao,
        safe_cast(data_particao as date) as data_particao
    from source
)

select * from renomeado
