{{
    config(
        alias="procedimentos_clinicos", 
        materialized="incremental",
        schema="brutos_prontuario_vitacare_historico",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day"
        }
    )
}}

WITH

    source_procedimentos_clinicos AS (
        SELECT 
            CONCAT(
                NULLIF(id_cnes, ''), 
                '.',
                NULLIF(REPLACE(acto_id, '.0', ''), '')
            ) AS id_prontuario_global,
            *
        FROM {{ source('brutos_prontuario_vitacare_historico_staging', 'procedimentos_clinicos') }} 
    ),


      -- Using window function to deduplicate procedimentos_clinicos
    procedimentos_clinicos_deduplicados AS (
        SELECT
            *
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY id_prontuario_global ORDER BY extracted_at DESC) AS rn
            FROM source_procedimentos_clinicos
        )
        WHERE rn = 1
    ),

    fato_procedimentos_clinicos AS (
        SELECT
            -- PKs e Chaves
            id_prontuario_global,
            REPLACE(acto_id, '.0', '') AS id_prontuario_local,
            id_cnes AS cnes_unidade,

            co_procedimento,
            {{ process_null('no_procedimento') }} AS no_procedimento,
   
            extracted_at AS loaded_at,
            DATE(SAFE_CAST(extracted_at AS DATETIME)) AS data_particao
        FROM procedimentos_clinicos_deduplicados
    )

SELECT
    *
FROM fato_procedimentos_clinicos