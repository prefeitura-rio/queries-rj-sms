{{
    config(
        alias="vacinas", 
        materialized="table",
        schema="brutos_prontuario_vitacare_historico",
    )
}}

WITH

    source_vacinas AS (
        SELECT 
            CONCAT(
                NULLIF({{ remove_double_quotes('id_cnes') }}, ''), 
                '.',
                NULLIF({{ remove_double_quotes('acto_id') }}, '')
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
            {{ remove_double_quotes('acto_id') }} AS id_prontuario_local,
            {{ remove_double_quotes('id_cnes') }} AS cnes_unidade,

           {{ process_null('nome_vacina') }} AS nome_vacina,
            cod_vacina AS cod_vacina,
            {{ process_null('dose') }} AS dose,
            lote AS lote,
            safe_cast('data_aplicacao' as DATETIME) AS data_aplicacao,
            safe_cast('data_registro' as DATETIME) AS data_registro,
            safe_cast({{ process_null('diff') }} as INT) AS diff,
            calendario_vacinal_atualizado AS calendario_vacinal_atualizado,
            tipo_registro AS tipo_registro,
            estrategia_imunizacao AS estrategia_imunizacao,
            foi_aplicada AS foi_aplicada,
            justificativa AS justificativa

            {{ remove_double_quotes('extracted_at') }} AS extracted_at
            
        FROM vacinas_deduplicados
    )

SELECT
    *
FROM fato_vacinas