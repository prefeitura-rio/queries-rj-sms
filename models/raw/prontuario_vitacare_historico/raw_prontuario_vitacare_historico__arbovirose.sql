{{
    config(
        alias="arbovirose", 
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

    source_arbovirose AS (
        SELECT 
            CONCAT(
                NULLIF(id_cnes, ''), 
                '.',
                NULLIF(REPLACE(acto_id, '.0', ''), '')
            ) as id_prontuario_global,
            *
        FROM {{ source('brutos_prontuario_vitacare_historico_staging', 'arboviroses') }} 
    ),


      -- Using window function to deduplicate arbovirose
    arbovirose_deduplicados AS (
        SELECT
            *
        FROM source_arbovirose 
        qualify row_number() over (partition by id_prontuario_global order by extracted_at desc) = 1 
    ),

    fato_arbovirose AS (
        SELECT
            -- PKs e Chaves
            id_prontuario_global,
            REPLACE(acto_id, '.0', '') as id_prontuario_local,
            id_cnes,

            safe_cast({{ process_null('symptomsstartdate') }} as datetime) as data_inicio_sintomas,
            {{ process_null('casedefinition') }} as caso_definicao,
            replace({{ process_null('feverdays') }}, '.0', '') as dias_com_febre,
            replace({{ process_null('vomitingepisodes') }}, '.0', '') as episodios_de_vomito,
            {{ process_null('persistentvomiting') }} as vomito_persistente,
            {{ process_null('alarmsymptoms') }} as alarm_symptoms,
            {{ process_null('symptomsobservations') }} as symptoms_observations,
            case 
                when {{ process_null('comorbiditiesvulnerability') }} = 'Sim' 
                then 1
                when {{ process_null('comorbiditiesvulnerability') }} = 'Não' 
                then 0
                else NULL
            end as comorbidities_vulnerability,
            {{ process_null('tourniquettest') }} as tourniquet_test,
            case 
                when {{ process_null('gingivalbleeding') }} = 'Sim' 
                then 1
                when {{ process_null('gingivalbleeding') }} = 'Não' 
                then 0
                else NULL
            end as gingival_bleeding,
            case 
                when {{ process_null('abdominalpalpationpain') }} = 'Sim' 
                then 1
                when {{ process_null('abdominalpalpationpain') }} = 'Não' 
                then 0
                else NULL
            end as abdominal_palpation_pain,
            case 
                when {{ process_null('enlargedliver') }} = 'Sim' 
                then 1
                when {{ process_null('enlargedliver') }} = 'Não' 
                then 0
                else NULL
            end as enlarged_liver,
            case 
                when {{ process_null('fluidaccumulation') }} = 'Sim' 
                then 1
                when {{ process_null('fluidaccumulation') }} = 'Não' 
                then 0
                else NULL
            end as fluid_accumulation,
            case 
                when {{ process_null('observedlethargy') }} = 'Sim' 
                then 1
                when {{ process_null('observedlethargy') }} = 'Não' 
                then 0
                else NULL
            end as observed_lethargy,
            case 
                when {{ process_null('observedirritability') }} = 'Sim' 
                then 1
                when {{ process_null('observedirritability') }} = 'Não' 
                then 0
                else NULL
            end as observed_irritability,
            {{ process_null('fluidaccumulationtype') }} as fluid_accumulation_type,
            {{ process_null('classificationgroup') }} as classification_group,
            replace({{ process_null('sinan') }}, '.0', '') as sinan,

            extracted_at AS loaded_at,
            DATE(SAFE_CAST(extracted_at AS DATETIME)) AS data_particao
        FROM arbovirose_deduplicados
    )

SELECT
    *
FROM fato_arbovirose