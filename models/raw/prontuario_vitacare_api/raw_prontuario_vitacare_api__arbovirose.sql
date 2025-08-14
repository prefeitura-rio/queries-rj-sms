{{ config(
    alias="arbovirose",
    materialized="incremental",
    schema="brutos_prontuario_vitacare_api",
    incremental_strategy="insert_overwrite",
    partition_by={"field": "data_particao", "data_type": "date", "granularity": "day"}
) }}

{% set last_partition = get_last_partition_date(this) %}

WITH bruto_atendimento AS (
  SELECT
    CAST(source_id AS STRING) AS id_prontuario_local,
    CAST(CONCAT(NULLIF(CAST(payload_cnes AS STRING), ''), '.', NULLIF(CAST(source_id AS STRING), '')) AS STRING) AS id_prontuario_global,
    CAST(payload_cnes AS STRING) AS id_cnes,
    SAFE_CAST(datalake_loaded_at AS DATETIME) AS loaded_at,
    data,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.datahora_fim_atendimento') AS DATETIME) AS datahora_fim_atendimento
  FROM {{ source("brutos_prontuario_vitacare_staging", "atendimento_continuo") }}
  {% if is_incremental() %}
    WHERE DATE(loaded_at, 'America/Sao_Paulo') >= DATE('{{ last_partition }}')
  {% endif %}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id_prontuario_global ORDER BY loaded_at DESC) = 1
),

arbovirose_extraida AS (
  SELECT
    id_prontuario_global,
    id_prontuario_local,
    id_cnes,

    SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.arbovirose[0].symptomsStartDate') AS DATETIME) AS data_inicio_sintomas,
    JSON_EXTRACT_SCALAR(data, '$.arbovirose[0].caseDefinition') AS definicao_caso,
    CAST(JSON_EXTRACT_SCALAR(data, '$.arbovirose[0].feverDays') AS STRING) AS dias_com_febre,
    JSON_EXTRACT_SCALAR(data, '$.arbovirose[0].vomitingEpisodes') AS episodios_de_vomito,
    JSON_EXTRACT_SCALAR(data, '$.arbovirose[0].persistentVomiting') AS vomito_persistente,
    JSON_EXTRACT_SCALAR(data, '$.arbovirose[0].alarmSymptoms') AS sintomas_de_alarme,
    JSON_EXTRACT_SCALAR(data, '$.arbovirose[0].symptomsObservations') AS observacoes_sintomas,

    CASE LOWER(JSON_EXTRACT_SCALAR(data, '$.arbovirose[0].comorbiditiesVulnerability'))
      WHEN 'sim' THEN 1
      WHEN 'não' THEN 0
      WHEN 'nao' THEN 0
      ELSE NULL
    END AS comorbidades_vulnerabilidade,

    JSON_EXTRACT_SCALAR(data, '$.arbovirose[0].tourniquetTest') AS teste_do_torniquete,

    CASE
      WHEN LOWER(JSON_EXTRACT_SCALAR(data, '$.arbovirose[0].gingivalBleeding')) = 'sim' THEN 1
      WHEN LOWER(JSON_EXTRACT_SCALAR(data, '$.arbovirose[0].gingivalBleeding')) IN ('não','nao') THEN 0
      ELSE NULL
    END AS sangramento_gengival,

    CASE LOWER(JSON_EXTRACT_SCALAR(data, '$.arbovirose[0].abdominalPalpationPain'))
      WHEN 'sim' THEN 1
      WHEN 'não' THEN 0
      WHEN 'nao' THEN 0
      ELSE NULL
    END AS dor_palpacao_abdominal,

    CASE LOWER(JSON_EXTRACT_SCALAR(data, '$.arbovirose[0].enlargedLiver'))
      WHEN 'sim' THEN 1
      WHEN 'não' THEN 0
      WHEN 'nao' THEN 0
      ELSE NULL
    END AS figado_aumentado,

    CASE LOWER(JSON_EXTRACT_SCALAR(data, '$.arbovirose[0].fluidAccumulation'))
      WHEN 'sim' THEN 1
      WHEN 'não' THEN 0
      WHEN 'nao' THEN 0
      ELSE NULL
    END AS acumulacao_liquido,

    CASE LOWER(JSON_EXTRACT_SCALAR(data, '$.arbovirose[0].observedLethargy'))
      WHEN 'sim' THEN 1
      WHEN 'não' THEN 0
      WHEN 'nao' THEN 0
      ELSE NULL
    END AS letargia_observada,

    CASE LOWER(JSON_EXTRACT_SCALAR(data, '$.arbovirose[0].observedIrritability'))
      WHEN 'sim' THEN 1
      WHEN 'não' THEN 0
      WHEN 'nao' THEN 0
      ELSE NULL
    END AS irritabilidade_observada,

    JSON_EXTRACT_SCALAR(data, '$.arbovirose[0].fluidAccumulationType') AS tipo_acumulacao_liquido,
    JSON_EXTRACT_SCALAR(data, '$.arbovirose[0].classificationGroup') AS grupo_classificacao,
    JSON_EXTRACT_SCALAR(data, '$.arbovirose[0].sinan') AS sinan,

    loaded_at,
    DATE(COALESCE(datahora_fim_atendimento, loaded_at)) AS data_particao
  FROM bruto_atendimento
)

SELECT *
FROM arbovirose_extraida