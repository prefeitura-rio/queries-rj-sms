{{
    config(
        alias="teste_rapido", 
        materialized="incremental",
        unique_key = 'id_prontuario_global',
        cluster_by= 'id_prontuario_global',
        schema="brutos_prontuario_vitacare_historico",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day"
        }
    )
}}

{% set last_partition = get_last_partition_date(this) %}

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
        {% if is_incremental() %}
            WHERE data_particao > '{{last_partition}}'
        {% endif %}
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

            {{ process_null('pregnancytestresult') }} AS resultado_teste_gravidez,
            {{ process_null('positivepregnancytestresult') }} AS resultado_teste_gravidez_positivo,
            {{ process_null('fastingglucose') }} AS glicose_jejum,
            {{ process_null('postprandialglucose') }} AS glicose_pos_prandial,
            {{ process_null('capillaryglucose') }} AS glicose_capilar,
            {{ process_null('syphilistestresult') }} AS resultado_teste_sifilis,
            {{ process_null('positivesyphilistestresult') }} AS resultado_teste_sifilis_positivo,
            {{ process_null('ppdresult') }} AS resultado_ppd,
            {{ process_null('ppdtestdate') }} AS data_teste_ppd,
            {{ process_null('hepatitisctestresult') }} AS resultado_teste_hepatite_c,
            {{ process_null('positivehepatitisctestresult') }} AS resultado_teste_hepatite_c_positivo,
            {{ process_null('tuberculosismoleculartestresult') }} AS resultado_teste_molecular_tuberculose,
            {{ process_null('hepatitisbtestresult') }} AS resultado_teste_hepatite_b,
            {{ process_null('positivehepatitisbtestresult') }} AS resultado_teste_hepatite_b_positivo,
            {{ process_null('sarscov2testresult') }} AS resultado_teste_sarscov2,
            {{ process_null('positivesarscov2testresult') }} AS resultado_teste_sarscov2_positivo,

            extracted_at AS loaded_at,
            DATE(SAFE_CAST(extracted_at AS DATETIME)) AS data_particao
            
        FROM testesrapidos_deduplicados
    ),

    -- Filtro temporário para remover registros anteriores à carga oficial (24/06/2025 17:15)
    fato_filtrado AS (
        SELECT *
        FROM fato_testesrapidos
        WHERE PARSE_TIMESTAMP('%F %H:%M:%E6S', loaded_at) > TIMESTAMP('2025-06-24 17:15:00.000000')
    )

SELECT
    *
FROM fato_filtrado