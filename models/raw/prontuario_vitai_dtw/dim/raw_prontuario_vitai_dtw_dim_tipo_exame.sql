{{
    config(
        alias="dim_tipo_exame",
        materialized="table",
        unique_key="tex_id"
    )
}}

with
source as (
    select *
    from {{ source("brutos_prontuario_vitai_dtw_staging", "dim_tipo_exame") }}
),

renomeado as (
    select
        safe_cast(tex_id as int64) as tex_id,
        safe_cast(tex_descricao as string) as tex_descricao,
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
