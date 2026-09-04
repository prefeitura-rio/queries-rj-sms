{{
    config(
        alias="fat_item_prescricao",
        materialized="incremental",
        unique_key="item_prescricao_gid"
    )
}}

with
source as (
    select *
    from {{ source("brutos_prontuario_vitai_dtw_staging", "fat_item_prescricao") }}
),

renomeado as (
    select
        safe_cast(item_prescrito as string) as item_prescrito,
        safe_cast(item_prescricao_gid as string) as item_prescricao_gid,
        safe_cast(observacao as string) as observacao,
        safe_cast(is_antibiotico as string) as is_antibiotico,
        safe_cast(tip_id as int64) as tip_id,
        safe_cast(via_id as int64) as via_id,
        safe_cast(quantidade as numeric) as quantidade,
        safe_cast(pri_descricaoitem as string) as pri_descricaoitem,
        safe_cast(estabelecimento_gid as string) as estabelecimento_gid,
        safe_cast(created_at as datetime) as created_at,
        safe_cast(boletim_gid as string) as boletim_gid,
        safe_cast(orientacao_uso as string) as orientacao_uso,
        safe_cast(unm_id as int64) as unm_id,
        safe_cast(datahora_cadastro as datetime) as datahora_cadastro,
        safe_cast(produto_associado as string) as produto_associado,
        safe_cast(prescricao_gid as string) as prescricao_gid,
        safe_cast(updated_at as datetime) as updated_at,

        -- Metadados
        datetime(timestamp(datalake_loaded_at), 'America/Sao_Paulo') as loaded_at,
        safe_cast(ano_particao as int64) as ano_particao,
        safe_cast(mes_particao as int64) as mes_particao,
        safe_cast(data_particao as date) as data_particao
    from source
)

select * from renomeado
