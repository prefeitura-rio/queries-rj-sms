{{
    config(
        alias="fat_sinais_vitais",
        materialized="incremental",
        unique_key="sinais_vitais_gid"
    )
}}

with
source as (
    select *
    from {{ source("brutos_prontuario_vitai_dtw_staging", "fat_sinais_vitais") }}
),

renomeado as (
    select
        safe_cast(asv_saturacao as numeric) as asv_saturacao,
        safe_cast(fat_paciente_rede_id as int64) as fat_paciente_rede_id,
        safe_cast(asv_freq_respiratoria as numeric) as asv_freq_respiratoria,
        safe_cast(estabelecimento_gid as string) as estabelecimento_gid,
        safe_cast(asv_pa_sistolica as numeric) as asv_pa_sistolica,
        safe_cast(asv_temp_corporal as numeric) as asv_temp_corporal,
        safe_cast(asv_escala_dor as int64) as asv_escala_dor,
        safe_cast(asv_freq_cardiaca as numeric) as asv_freq_cardiaca,
        safe_cast(updated_at as datetime) as updated_at,
        safe_cast(data_afericao as datetime) as data_afericao,
        safe_cast(asv_indice_glasgow as int64) as asv_indice_glasgow,
        safe_cast(asv_hemoglicoteste as numeric) as asv_hemoglicoteste,
        safe_cast(asv_pa_diastolica as numeric) as asv_pa_diastolica,
        safe_cast(cor_pele as string) as cor_pele,
        safe_cast(asv_peso as numeric) as asv_peso,
        safe_cast(datahora_cadastro as datetime) as datahora_cadastro,
        safe_cast(asv_altura as int64) as asv_altura,
        safe_cast(boletim_gid as string) as boletim_gid,
        safe_cast(paciente_gid as string) as paciente_gid,
        safe_cast(sinais_vitais_gid as string) as sinais_vitais_gid,
        safe_cast(codigo as int64) as codigo,
        safe_cast(datahora as datetime) as datahora,
        safe_cast(created_at as datetime) as created_at,
        safe_cast(prf_id as int64) as prf_id,

        -- Metadados
        datetime(timestamp(datalake_loaded_at), 'America/Sao_Paulo') as loaded_at,
        safe_cast(ano_particao as int64) as ano_particao,
        safe_cast(mes_particao as int64) as mes_particao,
        safe_cast(data_particao as date) as data_particao
    from source
)

select * from renomeado
