{{
    config(
        alias="fat_exame_item",
        materialized="incremental",
        unique_key="exi_id"
    )
}}

with
source as (
    select *
    from {{ source("brutos_prontuario_vitai_dtw_staging", "fat_exame_item") }}
),

renomeado as (
    select
        safe_cast(exi_id as int64) as exi_id,
        safe_cast(exame_id as int64) as exame_id,
        safe_cast(exame_gid as string) as exame_gid,
        safe_cast(data_exclusao as datetime) as data_exclusao,
        safe_cast(data_realizacao as datetime) as data_realizacao,
        safe_cast(created_at as datetime) as created_at,
        safe_cast(exame_mneumonico as string) as exame_mneumonico,
        safe_cast(data_liberacao as datetime) as data_liberacao,
        safe_cast(prc_id as int64) as prc_id,
        safe_cast(updated_at as datetime) as updated_at,
        safe_cast(sie_id as int64) as sie_id,
        safe_cast(exame_descricao as string) as exame_descricao,
        safe_cast(codigo_interno as int64) as codigo_interno,

        -- Metadados
        datetime(timestamp(datalake_loaded_at), 'America/Sao_Paulo') as loaded_at,
        safe_cast(ano_particao as int64) as ano_particao,
        safe_cast(mes_particao as int64) as mes_particao,
        safe_cast(data_particao as date) as data_particao
    from source
)

select * from renomeado
