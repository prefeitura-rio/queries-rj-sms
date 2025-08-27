{{ config(
    alias="curativo",
    materialized="incremental",
    schema="brutos_prontuario_vitacare_api",
    incremental_strategy="insert_overwrite",
    partition_by={"field": "data_particao", "data_type": "date", "granularity": "day"}
) }}

{% set last_partition = get_last_partition_date(this) %}

WITH bruto_atendimento AS (
  SELECT
    CAST(source_id AS STRING) AS id_prontuario_local,
    CONCAT(
      NULLIF(CAST(payload_cnes AS STRING), ''),
      '.',
      NULLIF(CAST(source_id AS STRING), '')
    ) AS id_prontuario_global,
    CAST(payload_cnes AS STRING) AS id_cnes,
    SAFE_CAST(datalake_loaded_at AS DATETIME) AS loaded_at,
    data,
    DATE(SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.datahora_fim_atendimento') AS DATETIME)) AS data_particao
  FROM {{ source("brutos_prontuario_vitacare_staging_dev", "atendimento_continuo") }}
  WHERE JSON_EXTRACT(data, '$.curativo') IS NOT NULL
  AND JSON_EXTRACT(data, '$.curativo') != '[]'
  {% if is_incremental() %}
    AND DATE(datalake_loaded_at, 'America/Sao_Paulo') >= DATE('{{ last_partition }}')
  {% endif %}
  qualify row_number() over (partition by id_prontuario_global order by loaded_at desc) = 1
),

curativo_extraido AS (
  SELECT
    id_prontuario_global,
    id_prontuario_local,
    id_cnes,

    JSON_EXTRACT_SCALAR(data, '$.curativo[0].areaCircundanteItens') AS areaCircundanteItens,
    JSON_EXTRACT_SCALAR(data, '$.curativo[0].posXy') AS posxy,
    JSON_EXTRACT_SCALAR(data, '$.curativo[0].infeccaoAssociada') AS infeccao_associada,
    JSON_EXTRACT_SCALAR(data, '$.curativo[0].identificacaoFerida') AS identificacao_ferida,
    JSON_EXTRACT_SCALAR(data, '$.curativo[0].tratamento') AS tratamento,

    JSON_EXTRACT_SCALAR(data, '$.curativo[0].iptbPasBraquialEsq') AS ipt_bpas_braquial_esq,
    JSON_EXTRACT_SCALAR(data, '$.curativo[0].iptbPasBraquialDir') AS ipt_bpas_braquial_dir,
    JSON_EXTRACT_SCALAR(data, '$.curativo[0].iptbPasPeEsq') AS ipt_bpas_pe_esq,
    JSON_EXTRACT_SCALAR(data, '$.curativo[0].iptbPasPeDir') AS ipt_bpas_pe_dir,
    JSON_EXTRACT_SCALAR(data, '$.curativo[0].iptbPasTibiaPostEsq') AS ipt_bpas_tibia_post_esq,
    JSON_EXTRACT_SCALAR(data, '$.curativo[0].iptbPasTibiaPostDir') AS ipt_bpas_tibia_post_dir,

    JSON_EXTRACT_SCALAR(data, '$.curativo[0].iptbEsq') AS ipt_b_esq,
    JSON_EXTRACT_SCALAR(data, '$.curativo[0].iptbDir') AS ipt_b_dir,

    JSON_EXTRACT_SCALAR(data, '$.curativo[0].iptbOrientacaoEsq') AS ipt_b_orientacao_esq,
    JSON_EXTRACT_SCALAR(data, '$.curativo[0].iptbOrientacaoDir') AS ipt_b_orientacao_dir,

    JSON_EXTRACT_SCALAR(data, '$.curativo[0].enfEnfcurativaObs') AS enfenf_curativa_obs,

    loaded_at,
    data_particao

  FROM bruto_atendimento
),

curativo_dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id_prontuario_global ORDER BY loaded_at DESC) AS rn
  FROM curativo_extraido
)

SELECT * EXCEPT (rn)
FROM curativo_dedup
WHERE rn = 1
