{{
    config(
        alias="prescricao", 
        materialized="incremental",
        incremental_strategy='merge', 
        unique_key=['id_prontuario_global'],
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
                NULLIF(id_cnes, ''), 
                '.',
                NULLIF(REPLACE(acto_id, '.0', ''), '')
            ) AS id_prontuario_global,
            *
        FROM {{ source('brutos_prontuario_vitacare_historico_staging', 'prescricoes') }} 
    ),


      -- Using window function to deduplicate prescricoes
    prescricoes_deduplicados AS (
        SELECT
            *
        FROM source_prescricoes
        qualify row_number() over (partition by id_prontuario_global, cod_medicamento order by extracted_at desc) = 1
    ),

    fato_prescricoes AS (
        SELECT
            -- PKs e Chaves
            id_prontuario_global,
            REPLACE(acto_id, '.0', '') AS id_prontuario_local,
            id_cnes,
            nome_medicamento AS medicamento_nome,
            cod_medicamento AS id_medicamento,
            posologia,
            safe_cast((quantidade) AS NUMERIC) AS quantidade,
            uso_continuado,
   
            extracted_at AS loaded_at,
            DATE(SAFE_CAST(extracted_at AS DATETIME)) AS data_particao

        FROM prescricoes_deduplicados
    ),

    -- Filtro temporário para remover registros anteriores à carga oficial (24/06/2025 17:15)
    fato_filtrado AS (
        SELECT *
        FROM fato_prescricoes
        WHERE PARSE_TIMESTAMP('%F %H:%M:%E6S', loaded_at) > TIMESTAMP('2025-06-24 17:15:00.000000')
    )

SELECT
    *
FROM fato_filtrado