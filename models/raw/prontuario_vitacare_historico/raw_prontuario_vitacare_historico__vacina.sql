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
        FROM {{ source('brutos_vitacare_historic_staging', 'VACINAS') }} 
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

            {{ remove_double_quotes('nome_vacina') }} AS nome_vacina,
            {{ remove_double_quotes('cod_vacina') }} AS cod_vacina,
            {{ remove_double_quotes('dose') }} AS dose,
            {{ remove_double_quotes('lote') }} AS lote,
            {{ remove_double_quotes('data_aplicacao') }} AS data_aplicacao,
            {{ remove_double_quotes('data_registro') }} AS data_registro,
            {{ remove_double_quotes('diff') }} AS diff,
            {{ remove_double_quotes('calendario_vacinal_atualizado') }} AS calendario_vacinal_atualizado,
            {{ remove_double_quotes('tipo_registro') }} AS tipo_registro,
            {{ remove_double_quotes('estrategia_imunizacao') }} AS estrategia_imunizacao,
            {{ remove_double_quotes('foi_aplicada') }} AS foi_aplicada,
            {{ remove_double_quotes('justificativa') }} AS justificativa,



            {{ remove_double_quotes('extracted_at') }} AS extracted_at
            
        FROM vacinas_deduplicados
    )

SELECT
    *
FROM fato_vacinas