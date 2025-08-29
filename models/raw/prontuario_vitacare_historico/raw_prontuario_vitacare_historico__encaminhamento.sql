{{
    config(
        alias="encaminhamento", 
        materialized="incremental",
        unique_key = ['id_prontuario_global','encaminhamento_especialidade'],
        cluster_by = ['id_prontuario_global','encaminhamento_especialidade'],
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
    source_encaminhamentos AS (
        SELECT 
            CONCAT(
                NULLIF(id_cnes, ''), 
                '.',
                NULLIF(REPLACE(acto_id, '.0', ''), '')
            ) AS id_prontuario_global,
            *
        FROM {{ source('brutos_prontuario_vitacare_historico_staging', 'encaminhamentos') }} 
        {% if is_incremental() %}
            WHERE data_particao > '{{last_partition}}'
        {% endif %}
    ),

      -- Using window function to deduplicate encaminhamentos
    encaminhamentos_deduplicados AS (
        SELECT
            *
        FROM source_encaminhamentos 
        qualify row_number() over (partition by id_prontuario_global, encaminhamento_especialidade order by extracted_at desc) = 1
    ),

    fato_encaminhamentos AS (
        SELECT
            -- PKs e Chaves
            id_prontuario_global,
            REPLACE(acto_id, '.0', '') AS id_prontuario_local,
            id_cnes,
            encaminhamento_especialidade,
            extracted_at AS loaded_at,
            DATE(SAFE_CAST(extracted_at AS DATETIME)) AS data_particao
        FROM encaminhamentos_deduplicados
    ),

    -- Filtro temporário para remover registros anteriores à carga oficial (24/06/2025 17:15)
    fato_filtrado AS (
        SELECT *
        FROM fato_encaminhamentos
        WHERE PARSE_TIMESTAMP('%F %H:%M:%E6S', loaded_at) > TIMESTAMP('2025-06-24 17:15:00.000000')
    )

SELECT
    *
FROM fato_filtrado