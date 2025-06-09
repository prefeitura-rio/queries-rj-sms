{{
    config(
        alias="encaminhamentos", 
        materialized="table",
        schema="brutos_prontuario_vitacare_historico",
    )
}}

WITH

    source_encaminhamentos AS (
        SELECT 
            CONCAT(
                NULLIF({{ remove_double_quotes('id_cnes') }}, ''), 
                '.',
                NULLIF({{ remove_double_quotes('acto_id') }}, '')
            ) AS id_prontuario_global,
            *
        FROM {{ source('brutos_prontuario_vitacare_historico', 'ENCAMINHAMENTOS') }} 
    ),


      -- Using window function to deduplicate encaminhamentos
    encaminhamentos_deduplicados AS (
        SELECT
            *
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY id_prontuario_global ORDER BY extracted_at DESC) AS rn
            FROM source_encaminhamentos
        )
        WHERE rn = 1
    ),

    fato_encaminhamentos AS (
        SELECT
            -- PKs e Chaves
            id_prontuario_global,
            {{ remove_double_quotes('acto_id') }} AS id_prontuario_local,
            {{ remove_double_quotes('id_cnes') }} AS cnes_unidade,

            {{ remove_double_quotes('encaminhamento_especialidade') }} AS encaminhamento_especialidade,
   
            {{ remove_double_quotes('extracted_at') }} AS extracted_at
        FROM encaminhamentos_deduplicados
    )

SELECT
    *
FROM fato_encaminhamentos