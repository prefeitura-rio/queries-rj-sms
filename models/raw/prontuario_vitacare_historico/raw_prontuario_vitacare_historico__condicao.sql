{{
    config(
        alias="condicoes", 
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

    source_condicoes AS (
        SELECT 
            CONCAT(
                NULLIF({{ remove_double_quotes('id_cnes') }}, ''), 
                '.',
                NULLIF({{ remove_double_quotes('acto_id') }}, '')
            ) AS id_prontuario_global,
            *
        FROM {{ source('brutos_prontuario_vitacare_historico_staging', 'condicoes') }} 
    ),


      -- Using window function to deduplicate condicoes
    condicoes_deduplicados AS (
        SELECT
            *
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY id_prontuario_global ORDER BY extracted_at DESC) AS rn
            FROM source_condicoes
        )
        WHERE rn = 1
    ),

    fato_condicoes AS (
        SELECT
            -- PKs e Chaves
            id_prontuario_global,
            REPLACE(acto_id, '.0', '') AS id_prontuario_local,
            id_cnes AS cnes_unidade,

            cod_cid10 AS cod_cid10,
            estado AS estado,
            SAFE_CAST((data_diagnostico) AS DATETIME) AS data_diagnostico,
   
   
            extracted_at AS loaded_at,
            DATE(SAFE_CAST(extracted_at AS DATETIME)) AS data_particao
        FROM condicoes_deduplicados
    )

SELECT
    *
FROM fato_condicoes