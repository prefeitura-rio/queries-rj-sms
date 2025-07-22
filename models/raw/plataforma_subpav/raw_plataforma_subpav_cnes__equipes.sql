{{
    config(
        alias        = "cnes__equipes",
        materialized = "table",
        tags         = ['subpav', 'cnes'],
        cluster_by   = ["competencia_id", "ine", "unidade_id"]
    )
}}

with
    source as (
        select *
        from {{ source("brutos_plataforma_subpav_staging", "subpav_cnes__equipes") }}
    ),

    sem_duplicatas as (
        select *
        from source
        qualify
        row_number() over (
            partition by competencia_id, ine
            order by coalesce(updated_at, created_at) DESC
        ) = 1
    ),

    extrair_informacoes as (
        select
            SAFE_CAST(id AS INT64) AS id,  -- bigint
            SAFE_CAST(ine AS INT64) AS ine,  -- int
            SAFE_CAST(cod_area AS INT64) AS cod_area,  -- int
            {{normalize_null('nm_referencia')}} AS nm_referencia,  -- varchar
            SAFE_CAST(dt_ativacao AS DATE) AS dt_ativacao,  -- date
            SAFE_CAST(dt_desativacao AS DATE) AS dt_desativacao,  -- date
            SAFE_CAST(tp_pop_assist_quilomb AS INT64) AS tp_pop_assist_quilomb,  -- tinyint
            SAFE_CAST(tp_pop_assist_assent AS INT64) AS tp_pop_assist_assent,  -- tinyint
            SAFE_CAST(tp_pop_assist_geral AS INT64) AS tp_pop_assist_geral,  -- tinyint
            SAFE_CAST(tp_pop_assist_escola AS INT64) AS tp_pop_assist_escola,  -- tinyint
            SAFE_CAST(tp_pop_assist_pronasci AS INT64) AS tp_pop_assist_pronasci,  -- tinyint
            SAFE_CAST(tp_pop_assist_indigena AS INT64) AS tp_pop_assist_indigena,  -- tinyint
            SAFE_CAST(tp_pop_assist_ribeirinha AS INT64) AS tp_pop_assist_ribeirinha,  -- tinyint
            SAFE_CAST(tp_pop_assist_situacao_rua AS INT64) AS tp_pop_assist_situacao_rua,  -- tinyint
            SAFE_CAST(tp_pop_assist_priv_liberdade AS INT64) AS tp_pop_assist_priv_liberdade,  -- tinyint
            SAFE_CAST(tp_pop_assist_conflito_lei AS INT64) AS tp_pop_assist_conflito_lei,  -- tinyint
            SAFE_CAST(tp_pop_assist_adol_conf_lei AS INT64) AS tp_pop_assist_adol_conf_lei,  -- tinyint
            {{normalize_null('co_prof_sus_preceptor')}} AS co_prof_sus_preceptor,  -- varchar
            SAFE_CAST(dt_atualiza AS DATE) AS dt_atualiza,  -- date
            SAFE_CAST(competencia_id AS INT64) AS competencia_id,  -- bigint
            SAFE_CAST(unidade_id AS INT64) AS unidade_id,  -- bigint
            SAFE_CAST(tipo_equipe_id AS INT64) AS tipo_equipe_id,  -- bigint
            SAFE_CAST(subtipo_equipe_id AS INT64) AS subtipo_equipe_id,  -- bigint
            SAFE_CAST(motivo_desativacao_equipe_id AS INT64) AS motivo_desativacao_equipe_id,  -- bigint
            SAFE_CAST(tipo_desativacao_id AS INT64) AS tipo_desativacao_id,  -- bigint
            SAFE_CAST(created_at AS TIMESTAMP) AS created_at,  -- timestamp
            SAFE_CAST(updated_at AS TIMESTAMP) AS updated_at  -- timestamp
        from sem_duplicatas
    )
select * from extrair_informacoes
