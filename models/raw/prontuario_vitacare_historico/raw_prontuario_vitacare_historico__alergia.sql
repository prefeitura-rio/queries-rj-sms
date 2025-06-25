{{
    config(
        alias="alergias", 
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

    source_alergias AS (
        SELECT 
            CONCAT(
                NULLIF({{ remove_double_quotes('id_cnes') }}, ''), 
                '.',
                NULLIF({{ remove_double_quotes('acto_id') }}, '')
            ) AS id_prontuario_global,
            *
        FROM {{ source('brutos_prontuario_vitacare_historico_staging', 'alergias') }} 
    ),


      -- Using window function to deduplicate alergias
    alergias_deduplicados AS (
        SELECT
            *
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY id_prontuario_global ORDER BY extracted_at DESC) AS rn
            FROM source_alergias
        )
        WHERE rn = 1
    ),

    fato_alergias AS (
        SELECT
            -- PKs e Chaves
            id_prontuario_global,
            acto_id AS id_prontuario_local,
            id_cnes AS cnes_unidade,

            alergias_anamnese_descricao,
   
            extracted_at AS loaded_at,
            DATE(SAFE_CAST(extracted_at AS DATETIME)) AS data_particao
        FROM alergias_deduplicados
    )

SELECT
    *
FROM fato_alergias