{{
    config(
        alias        = "principal__bairros",
        materialized = "table",
        tags         = ['subpav', 'bairros']
    )
}}

with
    source as (
        select 
            *
        from {{ source("brutos_plataforma_subpav_staging", "subpav_principal__bairros") }}
    ),

    sem_duplicatas as (
        select *
        from source
        qualify row_number() over (partition by id order by id desc) = 1 
    ),

    extrair_informacoes as (
        select
            SAFE_CAST(id AS INT64) AS id,  -- int
            descricao AS descricao,  -- varchar
            SAFE_CAST(cod_ra AS INT64) AS cod_ra,  -- int
            regiao_adm AS regiao_adm,  -- varchar
            SAFE_CAST(area_plane AS INT64) AS area_plane,  -- int
            SAFE_CAST(aps AS INT64) AS aps,  -- int
        from sem_duplicatas
    )
select * from extrair_informacoes
