{{
    config(
        alias="sifilis", 
        materialized="incremental",
        incremental_strategy='merge', 
        unique_key=['id_prontuario_global'],
        schema="brutos_prontuario_vitacare_historico",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day"
        }
    )
}}

WITH

    source_sifilis AS (
        SELECT 
            CONCAT(
                NULLIF(id_cnes, ''), 
                '.',
                NULLIF(REPLACE(acto_id, '.0', ''), '')
            ) AS id_prontuario_global,
            *
        FROM {{ source('brutos_prontuario_vitacare_historico_staging', 'sifilis') }} 
    ),


      -- Using window function to deduplicate sifilis
    sifilis_deduplicados AS (
        SELECT
            *
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY id_prontuario_global ORDER BY extracted_at DESC) AS rn
            FROM source_sifilis
        )
        WHERE rn = 1
    ),

    fato_sifilis AS (
        SELECT
            -- PKs e Chaves
            id_prontuario_global,
            REPLACE(acto_id, '.0', '') AS id_prontuario_local,
            id_cnes,

            safe_cast({{ process_null('acquiredsyphilissinannumber') }} as NUMERIC) AS acquiredsyphilissinannumber,
            {{ process_null('acquiredsyphilisclinicalclassification') }} AS acquiredsyphilisclinicalclassification,
            {{ process_null('acquiredsyphilistreatmentregimen') }} AS acquiredsyphilistreatmentregimen,
            safe_cast({{ process_null('acquiredsyphilistreatmentstartdate') }} as DATE) AS acquiredsyphilistreatmentstartdate,
            {{ process_null('acquiredsyphilisfinalclassification') }} AS acquiredsyphilisfinalclassification,
            {{ process_null('acquiredsyphilisfinalclassificationreason') }} AS acquiredsyphilisfinalclassificationreason,
            {{ process_null('acquiredsyphilisobservations') }} AS acquiredsyphilisobservations,
            safe_cast({{ process_null('acquiredsyphilisclosuredate') }} as DATE) AS acquiredsyphilisclosuredate,
            safe_cast({{ process_null('acquiredsyphilisageatclosure') }} as INT) AS acquiredsyphilisageatclosure,
            {{ process_null('acquiredsyphilisclosurereason') }} AS acquiredsyphilisclosurereason,

            extracted_at AS loaded_at,
            DATE(SAFE_CAST(extracted_at AS DATETIME)) AS data_particao
            
        FROM sifilis_deduplicados
    ),

    -- Filtro temporário para remover registros anteriores à carga oficial (24/06/2025 17:15)
    fato_filtrado AS (
        SELECT *
        FROM fato_sifilis
        WHERE PARSE_TIMESTAMP('%F %H:%M:%E6S', loaded_at) > TIMESTAMP('2025-06-24 17:15:00.000000')
    )

SELECT
    *
FROM fato_filtrado