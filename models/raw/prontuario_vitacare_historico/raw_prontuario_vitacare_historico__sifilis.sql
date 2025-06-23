{{
    config(
        alias="sifilis", 
        materialized="table",
        schema="brutos_prontuario_vitacare_historico",
    )
}}

WITH

    source_sifilis AS (
        SELECT 
            CONCAT(
                NULLIF({{ remove_double_quotes('id_cnes') }}, ''), 
                '.',
                NULLIF({{ remove_double_quotes('acto_id') }}, '')
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
            acto_id AS id_prontuario_local,
            id_cnes AS cnes_unidade, 

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

            extracted_at AS extracted_at
            
        FROM sifilis_deduplicados
    )

SELECT
    *
FROM fato_sifilis