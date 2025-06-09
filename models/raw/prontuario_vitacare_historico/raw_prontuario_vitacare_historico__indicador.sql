{{
    config(
        alias="indicadores", 
        materialized="table",
        schema="brutos_prontuario_vitacare_historico",
    )
}}

WITH

    source_indicadores AS (
        SELECT 
            CONCAT(
                NULLIF({{ remove_double_quotes('id_cnes') }}, ''), 
                '.',
                NULLIF({{ remove_double_quotes('acto_id') }}, '')
            ) AS id_prontuario_global,
            *
        FROM {{ source('brutos_prontuario_vitacare_historico', 'INDICADORES') }} 
    ),


      -- Using window function to deduplicate indicadores
    indicadores_deduplicados AS (
        SELECT
            *
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY id_prontuario_global ORDER BY extracted_at DESC) AS rn
            FROM source_indicadores
        )
        WHERE rn = 1
    ),

    fato_indicadores AS (
        SELECT
            -- PKs e Chaves
            id_prontuario_global,
            {{ remove_double_quotes('acto_id') }} AS id_prontuario_local,
            {{ remove_double_quotes('id_cnes') }} AS cnes_unidade,

            {{ remove_double_quotes('indicadores_nome') }} AS indicadores_nome,
            {{ remove_double_quotes('valor') }} AS valor,
   
            {{ remove_double_quotes('extracted_at') }} AS extracted_at
        FROM indicadores_deduplicados
    )

SELECT
    *
FROM fato_indicadores