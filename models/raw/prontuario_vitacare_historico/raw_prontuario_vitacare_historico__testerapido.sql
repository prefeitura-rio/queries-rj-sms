{{
    config(
        alias="testesrapidos", 
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

    source_testesrapidos AS (
        SELECT 
            CONCAT(
                NULLIF(id_cnes, ''), 
                '.',
                NULLIF(REPLACE(acto_id, '.0', ''), '')
            ) AS id_prontuario_global,
            *
        FROM {{ source('brutos_prontuario_vitacare_historico_staging', 'testesrapidos') }} 
    ),


      -- Using window function to deduplicate testesrapidos
    testesrapidos_deduplicados AS (
        SELECT
            *
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY id_prontuario_global ORDER BY extracted_at DESC) AS rn
            FROM source_testesrapidos
        )
        WHERE rn = 1
    ),

    fato_testesrapidos AS (
        SELECT
            -- PKs e Chaves
            id_prontuario_global,
            REPLACE(acto_id, '.0', '') AS id_prontuario_local,
            id_cnes, 

            {{ process_null('pregnancytestresult') }} AS pregnancytestresult,
            {{ process_null('positivepregnancytestresult') }} AS positivepregnancytestresult,
            {{ process_null('fastingglucose') }} AS fastingglucose,
            {{ process_null('postprandialglucose') }} AS postprandialglucose,
            {{ process_null('capillaryglucose') }} AS capillaryglucose,
            {{ process_null('syphilistestresult') }} AS syphilistestresult,
            {{ process_null('positivesyphilistestresult') }} AS positivesyphilistestresult,
            {{ process_null('ppdresult') }} AS ppdresult,
            {{ process_null('ppdtestdate') }} AS ppdtestdate,
            {{ process_null('hepatitisctestresult') }} AS hepatitisctestresult,
            {{ process_null('positivehepatitisctestresult') }} AS positivehepatitisctestresult,
            {{ process_null('tuberculosismoleculartestresult') }} AS tuberculosismoleculartestresult,
            {{ process_null('hepatitisbtestresult') }} AS hepatitisbtestresult,
            {{ process_null('positivehepatitisbtestresult') }} AS positivehepatitisbtestresult,
            {{ process_null('sarscov2testresult') }} AS sarscov2testresult,
            {{ process_null('positivesarscov2testresult') }} AS positivesarscov2testresult,

            extracted_at AS loaded_at,
            DATE(SAFE_CAST(extracted_at AS DATETIME)) AS data_particao
            
        FROM testesrapidos_deduplicados
    ),

    fato_filtrado AS (
        SELECT *
        FROM fato_testesrapidos
        WHERE PARSE_TIMESTAMP('%F %H:%M:%E6S', loaded_at) > TIMESTAMP('2025-06-24 17:15:00.000000')
    )

SELECT
    *
FROM fato_filtrado