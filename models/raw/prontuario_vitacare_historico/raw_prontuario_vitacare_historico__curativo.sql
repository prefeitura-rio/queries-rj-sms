{{
    config(
        alias="curativo", 
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

    source_curativos AS (
        SELECT 
            CONCAT(
                NULLIF(id_cnes, ''), 
                '.',
                NULLIF(REPLACE(acto_id, '.0', ''), '')
            ) AS id_prontuario_global,
            *
        FROM {{ source('brutos_prontuario_vitacare_historico_staging', 'curativos') }} 
        {% if is_incremental() %}
            WHERE data_particao > '{{last_partition}}'
        {% endif %}
    ),


      -- Using window function to deduplicate curativos
    curativos_deduplicados AS (
        SELECT
            *
        FROM source_curativos 
        qualify row_number() over (partition by id_prontuario_global order by extracted_at desc) = 1 
    ),

    fato_curativos AS (
        SELECT
            -- PKs e Chaves
            id_prontuario_global,
            REPLACE(acto_id, '.0', '') as id_prontuario_local,
            id_cnes,

            {{ process_null('posxy') }} as posxy,
            {{ process_null ('infeccaoassociada') }} as infeccao_associada,
            {{ process_null ('identificacaoferida') }} as identificacao_ferida,
            {{ process_null ('tratamento') }} as tratamento,
            {{ process_null ('iptbpasbraquialesq') }} as ipt_bpas_braquial_esq,
            {{ process_null ('iptbpasbraquialdir') }} as ipt_bpas_braquial_dir,
            {{ process_null ('iptbpaspeesq') }} as ipt_bpas_pe_esq,
            {{ process_null ('iptbpaspedir') }} as ipt_bpas_pe_dir,
            {{ process_null ('iptbpastibiapostesq') }} as ipt_bpas_tibia_post_esq,
            {{ process_null ('iptbpastibiapostdir') }} as ipt_bpas_tibia_post_dir,
            {{ process_null ('iptbesq') }} as ipt_b_esq,
            {{ process_null ('iptbdir') }} as ipt_b_dir,
            {{ process_null ('iptborientacaoesq') }} as ipt_b_orientacao_esq,
            {{ process_null ('iptborientacaodir') }} as ipt_b_orientacao_dir,
            {{ process_null ('enfenfcurativaobs') }} as enfenf_curativa_obs,

            extracted_at AS loaded_at,
            DATE(SAFE_CAST(extracted_at AS DATETIME)) AS data_particao
        FROM curativos_deduplicados
    )

SELECT
    *
FROM fato_curativos