{{
    config(
        alias        = "cnes__unidades",
        materialized = "table",
        tags         = ['subpav', 'cnes'],
        cluster_by   = ["cnes"]
    )
}}

with
    source as (
        select 
            *
        from {{ source("brutos_plataforma_subpav_staging", "subpav_cnes__unidades") }}
    ),

    sem_duplicatas as (
        select *
        from source
        qualify row_number() over (partition by id order by coalesce(updated_at, created_at) desc) = 1 
    ),

    extrair_informacoes as (
        select
            SAFE_CAST(id AS INT64) AS id,  -- bigint
            SAFE_CAST(cnes AS INT64) AS cnes,  -- int
            SAFE_CAST(ap AS INT64) AS ap,  -- int
            {{ normalize_null('nome_fanta') }} AS nome_fanta,  -- varchar
            {{ normalize_null('r_social') }} AS r_social,  -- varchar
            SAFE_CAST(dt_atualiza AS DATE) AS dt_atualiza,  -- date
            {{ normalize_null('tp_gestao') }} AS tp_gestao,  -- varchar
            SAFE_CAST(tp_estab_sempre_aberto AS INT64) AS tp_estab_sempre_aberto,  -- tinyint
            SAFE_CAST(dt_inaugura AS DATE) AS dt_inaugura,  -- date
            SAFE_CAST(tipo_unidade_id AS INT64) AS tipo_unidade_id,  -- bigint
            SAFE_CAST(cod_turnat_id AS INT64) AS cod_turnat_id,  -- bigint
            SAFE_CAST(motivo_desativacao_unidade_id AS INT64) AS motivo_desativacao_unidade_id,  -- bigint
            SAFE_CAST(natureza_juridica_id AS INT64) AS natureza_juridica_id,  -- bigint
            SAFE_CAST(tipo_estabelecimento_id AS INT64) AS tipo_estabelecimento_id,  -- bigint
            SAFE_CAST(atividade_principal_id AS INT64) AS atividade_principal_id,  -- bigint
            SAFE_CAST(prof_diretor_id AS INT64) AS prof_diretor_id,  -- bigint
            SAFE_CAST(created_at AS TIMESTAMP) AS created_at,  -- timestamp
            SAFE_CAST(updated_at AS TIMESTAMP) AS updated_at  -- timestamp
        from sem_duplicatas
    )
select * from extrair_informacoes
