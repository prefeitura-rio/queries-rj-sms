{{
    config(
        alias="testesrapidos", 
        materialized="table",
        schema="brutos_prontuario_vitacare_historico",
    )
}}

WITH

    source_testesrapidos AS (
        SELECT 
            CONCAT(
                NULLIF({{ remove_double_quotes('id_cnes') }}, ''), 
                '.',
                NULLIF({{ remove_double_quotes('acto_id') }}, '')
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
            {{ remove_double_quotes('acto_id') }} AS id_prontuario_local,
            {{ remove_double_quotes('id_cnes') }} AS cnes_unidade,

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
            {{ process_null('positivesarscov2testresult') }} AS positivesarscov2testresult

            {{ remove_double_quotes('extracted_at') }} AS extracted_at
            
        FROM testesrapidos_deduplicados
    )

SELECT
    *
FROM fato_testesrapidos