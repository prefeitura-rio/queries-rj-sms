{{
    config(
        alias="dim_faixa_etaria",
        materialized="table",
        unique_key="fae_id"
    )
}}

with
source as (
    select *
    from {{ source("brutos_prontuario_vitai_dtw_staging", "dim_faixa_etaria") }}
),

renomeado as (
    select
        safe_cast(fae_id as int64) as fae_id,
        safe_cast(fae_descricao as string) as fae_descricao,
        safe_cast(fae_idade_inicial as int64) as fae_idade_inicial,
        safe_cast(fae_idade_final as int64) as fae_idade_final,
        safe_cast(created_at as timestamp) as created_at,
        safe_cast(updated_at as timestamp) as updated_at,

        -- Metadados
        datetime(timestamp(datalake_loaded_at), 'America/Sao_Paulo') as loaded_at,
        safe_cast(ano_particao as int64) as ano_particao,
        safe_cast(mes_particao as int64) as mes_particao,
        safe_cast(data_particao as date) as data_particao
    from source
)

select * from renomeado
