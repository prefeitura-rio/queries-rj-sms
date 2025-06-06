{{
    config(
        alias="solicitacaoexames", 
        materialized="table",
        schema="brutos_prontuario_vitacare_historico",
    )
}}

WITH

    source_solicitacaoexames AS (
        SELECT 
            CONCAT(
                NULLIF({{ remove_double_quotes('id_cnes') }}, ''), 
                '.',
                NULLIF({{ remove_double_quotes('acto_id') }}, '')
            ) AS id_prontuario_global,
            *
        FROM {{ source('brutos_vitacare_historic_staging', 'SOLICITACAO_EXAMES') }} 
    ),


      -- Using window function to deduplicate solicitacaoexames
    solicitacaoexames_deduplicados AS (
        SELECT
            *
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY id_prontuario_global ORDER BY extracted_at DESC) AS rn
            FROM source_solicitacaoexames
        )
        WHERE rn = 1
    ),

    fato_solicitacaoexames AS (
        SELECT
            -- PKs e Chaves
            id_prontuario_global,
            {{ remove_double_quotes('acto_id') }} AS id_prontuario_local,
            {{ remove_double_quotes('id_cnes') }} AS cnes_unidade,

            {{ remove_double_quotes('nome_exame') }} AS nome_exame,
            {{ remove_double_quotes('cod_exame') }} AS cod_exame,
            {{ remove_double_quotes('quantidade') }} AS quantidade,
            {{ remove_double_quotes('material') }} AS material,
            {{ remove_double_quotes('data_solicitacao') }} AS data_solicitacao,

            {{ remove_double_quotes('extracted_at') }} AS extracted_at
            
        FROM solicitacaoexames_deduplicados
    )

SELECT
    *
FROM fato_solicitacaoexames