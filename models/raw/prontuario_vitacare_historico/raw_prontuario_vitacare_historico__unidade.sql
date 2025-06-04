{{
    config(
        alias="unidades", 
        materialized="table",
        schema="brutos_prontuario_vitacare_historico",
    )
}}

WITH

    source_unidades AS (
        SELECT  *
        FROM {{ source('brutos_vitacare_historic_staging', 'UNIDADE') }} 
    ),


      -- Using window function to deduplicate unidades
    unidades_deduplicados AS (
        SELECT
            *
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY id, unidade_ap, id_cnes ORDER BY extracted_at DESC) AS rn
            FROM source_unidades
        )
        WHERE rn = 1
    ),

    fato_unidades AS (
        SELECT
            -- PKs e Chaves
            {{ remove_double_quotes('id') }} AS id,
            {{ remove_double_quotes('id_cnes') }} AS cnes_unidades,
            {{ remove_double_quotes('unidade_nome') }} AS unidade_nome,
            {{ remove_double_quotes('unidade_ap') }} AS unidade_ap,
            {{ remove_double_quotes('tipo_entid') }} AS tipo_entid,

            {{ remove_double_quotes('extracted_at') }} AS extracted_at
            
        FROM unidades_deduplicados
    )

SELECT
    *
FROM fato_unidades