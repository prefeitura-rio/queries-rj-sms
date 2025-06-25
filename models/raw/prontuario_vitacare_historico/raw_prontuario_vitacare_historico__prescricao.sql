{{
    config(
        alias="prescricoes", 
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

    source_prescricoes AS (
        SELECT 
            CONCAT(
                NULLIF({{ remove_double_quotes('id_cnes') }}, ''), 
                '.',
                NULLIF({{ remove_double_quotes('acto_id') }}, '')
            ) AS id_prontuario_global,
            *
        FROM {{ source('brutos_prontuario_vitacare_historico_staging', 'prescricoes') }} 
    ),


      -- Using window function to deduplicate prescricoes
    prescricoes_deduplicados AS (
        SELECT
            *
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY id_prontuario_global ORDER BY extracted_at DESC) AS rn
            FROM source_prescricoes
        )
        WHERE rn = 1
    ),

    fato_prescricoes AS (
        SELECT
            -- PKs e Chaves
            id_prontuario_global,
            acto_id AS id_prontuario_local,
            {{ remove_double_quotes('id_cnes') }} AS cnes_unidade,
            nome_medicamento,
            cod_medicamento,
            posologia,
            safe_cast((quantidade) AS NUMERIC) AS quantidade,
            uso_continuado,
   
            extracted_at AS loaded_at,
            DATE(SAFE_CAST(extracted_at AS DATETIME)) AS data_particao

        FROM prescricoes_deduplicados
    )

SELECT
    *
FROM fato_prescricoes