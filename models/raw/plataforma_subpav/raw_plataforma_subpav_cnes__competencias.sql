{{
    config(
        alias="cnes__competencias",
        materialized="table",
        tags = ['subpav', 'cnes']
    )
}}

with
    source as (
        select 
            *
        from {{ source("brutos_plataforma_subpav_staging", "subpav_cnes__competencias") }}
    ),

    sem_duplicatas as (
        select *
        from source
        qualify row_number() over (partition by id order by updated_at desc) = 1 
    ),

    extrair_informacoes as (
        select
            SAFE_CAST(id AS INT64) AS id,  -- bigint
            ds_competencia AS ds_competencia,  -- varchar
            SAFE_CAST(dt_final AS DATE) AS dt_final,  -- date
            SAFE_CAST(created_at AS TIMESTAMP) AS created_at,  -- timestamp
            SAFE_CAST(updated_at AS TIMESTAMP) AS updated_at,  -- timestamp
            SAFE_CAST(base_final AS INT64) AS base_final  -- tinyint
        from sem_duplicatas
    )
select * from extrair_informacoes
