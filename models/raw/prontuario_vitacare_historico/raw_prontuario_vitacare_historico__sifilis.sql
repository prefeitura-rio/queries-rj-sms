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
        FROM {{ source('brutos_vitacare_historic_staging', 'SIFILIS') }} 
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
            {{ remove_double_quotes('acto_id') }} AS id_prontuario_local,
            {{ remove_double_quotes('id_cnes') }} AS cnes_unidade,

            {{ remove_double_quotes('acquiredsyphilissinannumber') }} AS acquiredsyphilissinannumber,
            {{ remove_double_quotes('acquiredsyphilisclinicalclassification') }} AS acquiredsyphilisclinicalclassification,
            {{ remove_double_quotes('acquiredsyphilistreatmentregimen') }} AS acquiredsyphilistreatmentregimen,
            {{ remove_double_quotes('acquiredsyphilistreatmentstartdate') }} AS acquiredsyphilistreatmentstartdate,
            {{ remove_double_quotes('acquiredsyphilisfinalclassification') }} AS acquiredsyphilisfinalclassification,
            {{ remove_double_quotes('acquiredsyphilisfinalclassificationreason') }} AS acquiredsyphilisfinalclassificationreason,
            {{ remove_double_quotes('acquiredsyphilisobservations') }} AS acquiredsyphilisobservations,
            {{ remove_double_quotes('acquiredsyphilisclosuredate') }} AS acquiredsyphilisclosuredate,
            {{ remove_double_quotes('acquiredsyphilisageatclosure') }} AS acquiredsyphilisageatclosure,
            {{ remove_double_quotes('acquiredsyphilisclosurereason') }} AS acquiredsyphilisclosurereason,

            {{ remove_double_quotes('extracted_at') }} AS extracted_at
            
        FROM sifilis_deduplicados
    )

SELECT
    *
FROM fato_sifilis