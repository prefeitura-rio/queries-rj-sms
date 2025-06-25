{{
    config(
        alias="equipes", 
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

    source_equipes AS (
        SELECT 
            *
        FROM {{ source('brutos_prontuario_vitacare_historico_staging', 'equipes') }} 
    ),


      -- Using window function to deduplicate equipes
    equipes_deduplicados AS (
        SELECT
            *
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY n_ine ORDER BY extracted_at DESC) AS rn
            FROM source_equipes
        )
        WHERE rn = 1
    ),

    fato_equipes AS (
        SELECT
            -- PKs e Chaves
            {{ remove_double_quotes('id_cnes') }} AS cnes_unidade,
            
            REPLACE(id, '.0', '') AS id,
            codigo,
            nome,
            n_ine,
   
            extracted_at AS loaded_at,
            DATE(SAFE_CAST(extracted_at AS DATETIME)) AS data_particao
        FROM equipes_deduplicados
    )

SELECT
    *
FROM fato_equipes