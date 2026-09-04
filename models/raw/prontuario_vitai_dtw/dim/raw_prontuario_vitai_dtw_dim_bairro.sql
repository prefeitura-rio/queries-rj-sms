{{
    config(
        alias="dim_bairro",
        materialized="table",
        unique_key="bai_id"
    )
}}

with
source as (
    select *
    from {{ source("brutos_prontuario_vitai_dtw_staging", "dim_bairro") }}
),

renomeado as (
    select
        safe_cast(bai_id as int64) as bai_id,
        safe_cast(bai_descricao as string) as bai_descricao,
        safe_cast(mun_id as int64) as mun_id,
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
