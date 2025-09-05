{{
    config(
        alias="sifilis", 
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

    source_sifilis AS (
        SELECT 
            CONCAT(
                NULLIF(id_cnes, ''), 
                '.',
                NULLIF(REPLACE(acto_id, '.0', ''), '')
            ) AS id_prontuario_global,
            *
        FROM {{ source('brutos_prontuario_vitacare_historico_staging', 'sifilis') }} 
        {% if is_incremental() %}
            WHERE data_particao > '{{last_partition}}'
        {% endif %}
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

            safe_cast({{ process_null('acquiredsyphilissinannumber') }} as NUMERIC) AS sifilis_adquirida_sinan_numero,
            {{ process_null('acquiredsyphilisclinicalclassification') }} AS sifilis_adquirida_classificacao_clinica,
            {{ process_null('acquiredsyphilistreatmentregimen') }} AS sifilis_adquirida_tratamento_esquema,
            safe_cast({{ process_null('acquiredsyphilistreatmentstartdate') }} as DATE) AS sifilis_adquirida_tratamento_data_inicio,
            {{ process_null('acquiredsyphilisfinalclassification') }} AS sifilis_adquirida_classificacao_final,
            {{ process_null('acquiredsyphilisfinalclassificationreason') }} AS sifilis_adquirida_classificacao_final_motivo,
            {{ process_null('acquiredsyphilisobservations') }} AS sifilis_adquirida_observacoes,
            safe_cast({{ process_null('acquiredsyphilisclosuredate') }} as DATE) AS sifilis_adquirida_data_fechamento,
            safe_cast({{ process_null('acquiredsyphilisageatclosure') }} as INT) AS sifilis_adquirida_idade_fechamento,
            {{ process_null('acquiredsyphilisclosurereason') }} AS sifilis_adquirida_motivo_fechamento,

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