{{
    config(
        alias="equipes", 
        materialized="table",
        schema="brutos_prontuario_vitacare_historico",
    )
}}

WITH

    source_equipes AS (
        SELECT 
            *
        FROM {{ source('brutos_prontuario_vitacare_historico', 'EQUIPES') }} 
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
            
            REPLACE({{ remove_double_quotes('id') }}, '.0', '') AS id,
            {{ remove_double_quotes('codigo') }} AS codigo,
            {{ remove_double_quotes('nome') }} AS nome,
            {{ remove_double_quotes('n_ine') }} AS n_ine,
   
            {{ remove_double_quotes('extracted_at') }} AS extracted_at
        FROM equipes_deduplicados
    )

SELECT
    *
FROM fato_equipes