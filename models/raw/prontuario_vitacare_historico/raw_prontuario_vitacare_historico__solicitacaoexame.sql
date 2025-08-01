{{
    config(
        alias="solicitacao_exame", 
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

    source_solicitacaoexames AS (
        SELECT 
            CONCAT(
                NULLIF(id_cnes, ''), 
                '.',
                NULLIF(REPLACE(acto_id, '.0', ''), '')
            ) AS id_prontuario_global,
            *
        FROM {{ source('brutos_prontuario_vitacare_historico_staging', 'solicitacao_exames') }} 
    ),


      -- Using window function to deduplicate solicitacaoexames
    solicitacaoexames_deduplicados AS (
        SELECT
            *
        FROM source_solicitacaoexames 
        qualify row_number() over (partition by id_prontuario_global, cod_exame order by extracted_at desc) = 1
    ),

    fato_solicitacaoexames AS (
        SELECT
            -- PKs e Chaves
            id_prontuario_global,
            REPLACE(acto_id, '.0', '') AS id_prontuario_local,
            id_cnes,
     
            {{ process_null('nome_exame') }} AS nome_exame,
            cod_exame AS cod_exame,
            safe_cast(quantidade as NUMERIC) AS quantidade,
            {{ process_null('material') }} AS material,
            safe_cast(data_solicitacao as DATETIME) AS data_solicitacao,

            extracted_at AS loaded_at,
            DATE(SAFE_CAST(extracted_at AS DATETIME)) AS data_particao
            
        FROM solicitacaoexames_deduplicados
    ),

    -- Filtro temporário para remover registros anteriores à carga oficial (24/06/2025 17:15)
    fato_filtrado AS (
        SELECT *
        FROM fato_solicitacaoexames
        WHERE PARSE_TIMESTAMP('%F %H:%M:%E6S', loaded_at) > TIMESTAMP('2025-06-24 17:15:00.000000')
    )

SELECT
    *
FROM fato_filtrado