{{
    config(
        alias="unidades", 
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

    source_unidades AS (
        SELECT  *
        FROM {{ source('brutos_prontuario_vitacare_historico_staging', 'unidade') }} 
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
            id,
            id_cnes cnes_unidade,
            unidade_nome,
            unidade_ap,
            {{ process_null('tipo_entid') }} AS tipo_entid,

            extracted_at AS loaded_at,
            DATE(SAFE_CAST(extracted_at AS DATETIME)) AS data_particao
            
        FROM unidades_deduplicados
    )

SELECT
    *
FROM fato_unidades