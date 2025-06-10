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
        FROM {{ source('brutos_prontuario_vitacare_historico_staging', 'TESTESRAPIDOS') }} 
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

            {{ process_null(remove_double_quotes('pregnancytestresult')) }} AS pregnancytestresult,
            {{ process_null(remove_double_quotes('positivepregnancytestresult')) }} AS positivepregnancytestresult,
            {{ process_null(remove_double_quotes('fastingglucose')) }} AS fastingglucose,
            {{ process_null(remove_double_quotes('postprandialglucose')) }} AS postprandialglucose,
            {{ process_null(remove_double_quotes('capillaryglucose')) }} AS capillaryglucose,
            {{ process_null(remove_double_quotes('syphilistestresult')) }} AS syphilistestresult,
            {{ process_null(remove_double_quotes('positivesyphilistestresult')) }} AS positivesyphilistestresult,
            {{ process_null(remove_double_quotes('ppdresult')) }} AS ppdresult,
            {{ process_null(remove_double_quotes('ppdtestdate')) }} AS ppdtestdate,
            {{ process_null(remove_double_quotes('hepatitisctestresult')) }} AS hepatitisctestresult,
            {{ process_null(remove_double_quotes('positivehepatitisctestresult')) }} AS positivehepatitisctestresult,
            {{ process_null(remove_double_quotes('tuberculosismoleculartestresult')) }} AS tuberculosismoleculartestresult,
            {{ process_null(remove_double_quotes('hepatitisbtestresult')) }} AS hepatitisbtestresult,
            {{ process_null(remove_double_quotes('positivehepatitisbtestresult')) }} AS positivehepatitisbtestresult,
            {{ process_null(remove_double_quotes('sarscov2testresult')) }} AS sarscov2testresult,
            {{ process_null(remove_double_quotes('positivesarscov2testresult')) }} AS positivesarscov2testresult,

            {{ remove_double_quotes('extracted_at') }} AS extracted_at
            
        FROM testesrapidos_deduplicados
    )

SELECT
    *
FROM fato_testesrapidos