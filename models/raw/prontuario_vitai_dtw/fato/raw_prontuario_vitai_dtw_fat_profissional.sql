{{
    config(
        alias="fat_profissional",
        materialized="incremental",
        unique_key="prf_id"
    )
}}

with
source as (
    select *
    from {{ source("brutos_prontuario_vitai_dtw_staging", "fat_profissional") }}
),

renomeado as (
    select
        safe_cast(datahora as datetime) as datahora,
        safe_cast(created_at as datetime) as created_at,
        safe_cast(situacao as string) as situacao,
        safe_cast(cpf as string) as cpf,
        safe_cast(updated_at as datetime) as updated_at,
        safe_cast(prf_id as int64) as prf_id,
        safe_cast(uf_conselho as string) as uf_conselho,
        safe_cast(cns as string) as cns,
        safe_cast(numero_conselho as string) as numero_conselho,
        safe_cast(nome as string) as nome,
        safe_cast(cre_id as int64) as cre_id,

        -- Metadados
        datetime(timestamp(datalake_loaded_at), 'America/Sao_Paulo') as loaded_at,
        safe_cast(ano_particao as int64) as ano_particao,
        safe_cast(mes_particao as int64) as mes_particao,
        safe_cast(data_particao as date) as data_particao
    from source
)

select * from renomeado
