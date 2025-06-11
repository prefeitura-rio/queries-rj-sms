{{
    config(
        alias="prescricoes", 
        materialized="table",
        schema="brutos_prontuario_vitacare_historico",
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
            {{ remove_double_quotes('acto_id') }} AS id_prontuario_local,
            {{ remove_double_quotes('id_cnes') }} AS cnes_unidade,
            {{ remove_double_quotes('nome_medicamento') }} AS nome_medicamento,
            {{ remove_double_quotes('cod_medicamento') }} AS cod_medicamento,
            {{ remove_double_quotes('posologia') }} AS posologia,
            {{ remove_double_quotes('quantidade') }} AS quantidade,
            {{ remove_double_quotes('uso_continuado') }} AS uso_continuado,
   
            {{ remove_double_quotes('extracted_at') }} AS extracted_at

        FROM prescricoes_deduplicados
    )

SELECT
    *
FROM fato_prescricoes