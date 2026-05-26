{{
    config(
        alias="equipe", 
        materialized="incremental",
        unique_key = 'n_ine',
        cluster_by= 'n_ine',
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

    source_equipes AS (
        SELECT 
            *
        FROM {{ source('brutos_prontuario_vitacare_historico_staging', 'equipes') }} 
        {% if is_incremental() %}
            WHERE data_particao > '{{last_partition}}'
        {% endif %}
    ),


      -- Using window function to deduplicate equipes
    equipes_deduplicados AS (
        SELECT
            *
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY n_ine ORDER BY extracted_at DESC) AS rn
            FROM source_equipes
        )
        WHERE rn = 1
    ),

    fato_equipes AS (
        SELECT
            -- PKs e Chaves
            id_cnes,
            
            REPLACE(id, '.0', '') AS id,
            codigo,
            nome,
            n_ine,
   
            cast({{ process_null('extracted_at') }} as datetime) as loaded_at,
            DATE(SAFE_CAST(extracted_at AS DATETIME)) AS data_particao
        FROM equipes_deduplicados
    )



SELECT
    *
FROM fato_equipes