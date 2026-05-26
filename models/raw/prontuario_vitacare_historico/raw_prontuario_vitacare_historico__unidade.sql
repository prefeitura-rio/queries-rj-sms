{{
    config(
        alias="unidade", 
        materialized="incremental",
        unique_key = ['id', 'unidade_ap', 'id_cnes'],
        cluster_by= ['id', 'unidade_ap', 'id_cnes'],
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

    source_unidades AS (
        SELECT  *
        FROM {{ source('brutos_prontuario_vitacare_historico_staging', 'unidade') }} 
        {% if is_incremental() %}
            WHERE data_particao > '{{last_partition}}'
        {% endif %}
    ),


      -- Using window function to deduplicate unidades
    unidades_deduplicados AS (
        SELECT
            *
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY id, unidade_ap, id_cnes ORDER BY extracted_at DESC) AS rn
            FROM source_unidades
        )
        WHERE rn = 1
    ),

    fato_unidades AS (
        SELECT
            -- PKs e Chaves
            id_cnes,
            replace(id, '.0', '') as id,
            unidade_nome,
            unidade_ap,
            {{ process_null('tipo_entid') }} AS tipo_entid,

            cast({{ process_null('extracted_at') }} as datetime) as loaded_at,
            DATE(SAFE_CAST(extracted_at AS DATETIME)) AS data_particao
            
        FROM unidades_deduplicados
    )


SELECT
    *
FROM fato_unidades