{{
    config(
        alias="profissionais", 
        materialized="table",
        schema="brutos_prontuario_vitacare_historico",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day"
        }
    )
}}

WITH

    source_profissionais AS (
        SELECT 
            *
        FROM {{ source('brutos_prontuario_vitacare_historico_staging', 'profissionais') }} 
    ),



    fato_profissionais AS (
        SELECT
            -- PKs e Chaves
            REPLACE(prof_id, '.0', '') AS prof_id,
            profissional_cns AS profissional_cns,
            profissional_cpf AS profissional_cpf,
            profissional_nome AS profissional_nome,
            n_registro AS n_registro,
            profissional_cbo AS profissional_cbo,
            profissional_cbo_descricao AS profissional_cbo_descricao,
            profissional_equipe_nome AS profissional_equipe_nome,
            profissional_equipe_cod_equipe AS profissional_equipe_cod_equipe,
            profissional_equipe_cod_ine AS profissional_equipe_cod_ine,
   
            extracted_at AS loaded_at,
            DATE(SAFE_CAST(extracted_at AS DATETIME)) AS data_particao

        FROM source_profissionais
    )

SELECT
    *
FROM fato_profissionais