{{
    config(
        alias="fat_exame_pedido",
        materialized="incremental",
        unique_key="pedido_id"
    )
}}

with
source as (
    select *
    from {{ source("brutos_prontuario_vitai_dtw_staging", "fat_exame_pedido") }}
),

renomeado as (
    select
        safe_cast(pedido_id as int64) as pedido_id,
        safe_cast(tex_id as int64) as tex_id,
        safe_cast(data_pedido as datetime) as data_pedido,
        safe_cast(updated_at as datetime) as updated_at,
        safe_cast(prf_id as int64) as prf_id,
        safe_cast(created_at as datetime) as created_at,
        safe_cast(exame_id as int64) as exame_id,
        safe_cast(paciente_gid as string) as paciente_gid,
        safe_cast(estabelecimento_gid as string) as estabelecimento_gid,
        safe_cast(boletim_gid as string) as boletim_gid,
        safe_cast(sk_data_pedido as int64) as sk_data_pedido,
        safe_cast(fat_paciente_rede_id as int64) as fat_paciente_rede_id,
        safe_cast(spe_id as int64) as spe_id,

        -- Metadados
        datetime(timestamp(datalake_loaded_at), 'America/Sao_Paulo') as loaded_at,
        safe_cast(ano_particao as int64) as ano_particao,
        safe_cast(mes_particao as int64) as mes_particao,
        safe_cast(data_particao as date) as data_particao
    from source
)

select * from renomeado
