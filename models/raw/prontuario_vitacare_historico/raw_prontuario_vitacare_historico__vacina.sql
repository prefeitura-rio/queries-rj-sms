{{
    config(
        alias="vacinas", 
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

    source_vacinas AS (
        SELECT 
            CONCAT(
                NULLIF(id_cnes, ''), 
                '.',
                NULLIF(REPLACE(acto_id, '.0', ''), '')
            ) AS id_prontuario_global,
            *
        FROM {{ source('brutos_prontuario_vitacare_historico_staging', 'vacinas') }} 
    ),


      -- Using window function to deduplicate vacinas
    vacinas_deduplicados AS (
        SELECT
            *
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY id_prontuario_global ORDER BY extracted_at DESC) AS rn
            FROM source_vacinas
        )
        WHERE rn = 1
    ),

    fato_vacinas AS (
        SELECT
            -- PKs e Chaves
            id_prontuario_global,
            REPLACE(acto_id, '.0', '') AS id_prontuario_local,
            id_cnes, 

            {{ process_null('nome_vacina') }} AS nome_vacina,
            cod_vacina AS cod_vacina,
            {{ process_null('dose') }} AS dose,
            lote AS lote,
            safe_cast('data_aplicacao' as DATETIME) AS data_aplicacao,
            safe_cast('data_registro' as DATETIME) AS data_registro,
            safe_cast({{ process_null('diff') }} as INT) AS diff,
            calendario_vacinal_atualizado AS calendario_vacinal_atualizado,
            {{ process_null('tipo_registro') }} AS tipo_registro,
            {{ process_null('estrategia_imunizacao') }} AS estrategia_imunizacao,
            {{ process_null('foi_aplicada') }} AS foi_aplicada,
            {{ process_null('justificativa') }} AS justificativa,

            extracted_at AS loaded_at,
            DATE(SAFE_CAST(extracted_at AS DATETIME)) AS data_particao
            
        FROM vacinas_deduplicados
    ),

    -- Filtro temporário para remover registros anteriores à carga oficial (24/06/2025 17:15)
    fato_filtrado AS (
        SELECT *
        FROM fato_vacinas
        WHERE PARSE_TIMESTAMP('%F %H:%M:%E6S', loaded_at) > TIMESTAMP('2025-06-24 17:15:00.000000')
    )

SELECT
    *
FROM fato_filtrado

