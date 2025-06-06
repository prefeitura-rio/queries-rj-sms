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
        FROM {{ source('brutos_vitacare_historic_staging', 'TESTESRAPIDOS') }} 
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

            {{ remove_double_quotes('pregnancytestresult') }} AS pregnancytestresult,
            {{ remove_double_quotes('positivepregnancytestresult') }} AS positivepregnancytestresult,
            {{ remove_double_quotes('fastingglucose') }} AS fastingglucose,
            {{ remove_double_quotes('postprandialglucose') }} AS postprandialglucose,
            {{ remove_double_quotes('capillaryglucose') }} AS capillaryglucose,
            {{ remove_double_quotes('syphilistestresult') }} AS syphilistestresult,
            {{ remove_double_quotes('positivesyphilistestresult') }} AS positivesyphilistestresult,
            {{ remove_double_quotes('ppdresult') }} AS ppdresult,
            {{ remove_double_quotes('ppdtestdate') }} AS ppdtestdate,
            {{ remove_double_quotes('hepatitisctestresult') }} AS hepatitisctestresult,
            {{ remove_double_quotes('positivehepatitisctestresult') }} AS positivehepatitisctestresult,
            {{ remove_double_quotes('tuberculosismoleculartestresult') }} AS tuberculosismoleculartestresult,
            {{ remove_double_quotes('hepatitisbtestresult') }} AS hepatitisbtestresult,
            {{ remove_double_quotes('positivehepatitisbtestresult') }} AS positivehepatitisbtestresult,
            {{ remove_double_quotes('sarscov2testresult') }} AS sarscov2testresult,
            {{ remove_double_quotes('positivesarscov2testresult') }} AS positivesarscov2testresult,

            {{ remove_double_quotes('extracted_at') }} AS extracted_at
            
        FROM testesrapidos_deduplicados
    )

SELECT
    *
FROM fato_testesrapidos