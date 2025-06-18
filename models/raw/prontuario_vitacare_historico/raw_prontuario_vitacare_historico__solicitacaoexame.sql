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
        FROM {{ source('brutos_prontuario_vitacare_historico_staging', 'solicitacao_exames') }} 
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
            acto_id AS id_prontuario_local,
            id_cnes AS cnes_unidade,
     
            {{ process_null('nome_exame') }} AS nome_exame,
            cod_exame AS cod_exame,
            safe_cast('quantidade' as NUMERIC(16,4)) AS quantidade,
            {{ process_null('material') }} AS material,
            safe_cast('data_solicitacao' as DATETIME) AS data_solicitacao,

            extracted_at
            
        FROM solicitacaoexames_deduplicados
    )

SELECT
    *
FROM fato_solicitacaoexames