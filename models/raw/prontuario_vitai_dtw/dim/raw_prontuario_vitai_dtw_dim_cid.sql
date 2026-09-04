{{
    config(
        schema="brutos_prontuario_vitai_dtw",
        alias="dim_cid",
        materialized="table",
        unique_key="cid_id"
    )
}}

with
source as (
    select *
    from {{ source("brutos_prontuario_vitai_dtw_staging", "dim_cid") }}
),

renomeado as (
    select
        safe_cast(cid_id as int64) as cid_id,
        safe_cast(cid_codigo as string) as cid_codigo,
        safe_cast(cid_descricao as string) as cid_descricao,
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
