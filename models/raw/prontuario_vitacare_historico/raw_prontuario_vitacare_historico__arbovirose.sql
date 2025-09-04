{{
    config(
        alias="arbovirose", 
        materialized="incremental",
        unique_key = 'id_prontuario_global',
        schema="brutos_prontuario_vitacare_historico",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by= 'id_prontuario_global'
    )
}}

{% set last_partition = get_last_partition_date(this) %}

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
        {% if is_incremental() %}
            WHERE data_particao > '{{last_partition}}'
        {% endif %}
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
            {{ process_null('casedefinition') }} as definicao_caso,
            replace({{ process_null('feverdays') }}, '.0', '') as dias_com_febre,
            replace({{ process_null('vomitingepisodes') }}, '.0', '') as episodios_de_vomito,
            {{ process_null('persistentvomiting') }} as vomito_persistente,
            {{ process_null('alarmsymptoms') }} as sintomas_de_alarme,
            {{ process_null('symptomsobservations') }} as observacoes_sintomas,
            case 
                when {{ process_null('comorbiditiesvulnerability') }} = 'Sim' 
                then 1
                when {{ process_null('comorbiditiesvulnerability') }} = 'Não' 
                then 0
                else NULL
            end as comorbidades_vulnerabilidade,
            {{ process_null('tourniquettest') }} as teste_do_torniquete,
            case 
                when {{ process_null('gingivalbleeding') }} = 'Sim' 
                then 1
                when {{ process_null('gingivalbleeding') }} = 'Não' 
                then 0
                else NULL
            end as sangramento_gengival,
            case 
                when {{ process_null('abdominalpalpationpain') }} = 'Sim' 
                then 1
                when {{ process_null('abdominalpalpationpain') }} = 'Não' 
                then 0
                else NULL
            end as dor_palpacao_abdominal,
            case 
                when {{ process_null('enlargedliver') }} = 'Sim' 
                then 1
                when {{ process_null('enlargedliver') }} = 'Não' 
                then 0
                else NULL
            end as figado_aumentado,
            case 
                when {{ process_null('fluidaccumulation') }} = 'Sim' 
                then 1
                when {{ process_null('fluidaccumulation') }} = 'Não' 
                then 0
                else NULL
            end as acumulacao_liquido,
            case 
                when {{ process_null('observedlethargy') }} = 'Sim' 
                then 1
                when {{ process_null('observedlethargy') }} = 'Não' 
                then 0
                else NULL
            end as letargia_observada,
            case 
                when {{ process_null('observedirritability') }} = 'Sim' 
                then 1
                when {{ process_null('observedirritability') }} = 'Não' 
                then 0
                else NULL
            end as irritabilidade_observada,
            {{ process_null('fluidaccumulationtype') }} as tipo_acumulacao_liquido,
            {{ process_null('classificationgroup') }} as grupo_classificacao,
            replace({{ process_null('sinan') }}, '.0', '') as sinan,

            extracted_at AS loaded_at,
            DATE(SAFE_CAST(extracted_at AS DATETIME)) AS data_particao
        FROM arbovirose_deduplicados
    )

SELECT
    *
FROM fato_arbovirose