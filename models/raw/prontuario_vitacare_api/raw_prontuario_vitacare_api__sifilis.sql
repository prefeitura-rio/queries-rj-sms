{{ config(
    alias="sifilis",
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
  FROM {{ source("brutos_prontuario_vitacare_staging", "atendimento_continuo") }}
  {% if is_incremental() %}
    WHERE DATE(datalake_loaded_at, 'America/Sao_Paulo') >= DATE('{{ last_partition }}')
  {% endif %}
  qualify row_number() over(partition by id_prontuario_global order by datalake_loaded_at desc) = 1
),

base_sifilis_extraida AS (
  SELECT
    id_prontuario_global,
    id_prontuario_local,
    id_cnes,

    SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.syphilis_adquirida[0].acquiredSyphilisSinanNumber') AS NUMERIC) AS acquiredsyphilissinannumber,
    JSON_EXTRACT_SCALAR(data, '$.syphilis_adquirida[0].acquiredSyphilisClinicalClassification') AS acquiredsyphilisclinicalclassification,
    JSON_EXTRACT_SCALAR(data, '$.syphilis_adquirida[0].acquiredSyphilisTreatmentRegimen') AS acquiredsyphilistreatmentregimen,
    PARSE_DATETIME('%b %d, %Y %I:%M:%S %p', JSON_EXTRACT_SCALAR(data, '$.syphilis_adquirida[0].acquiredSyphilisTreatmentStartDate')) AS acquiredsyphilistreatmentstartdatetime,
    JSON_EXTRACT_SCALAR(data, '$.syphilis_adquirida[0].acquiredSyphilisFinalClassification') AS acquiredsyphilisfinalclassification,
    JSON_EXTRACT_SCALAR(data, '$.syphilis_adquirida[0].acquiredSyphilisFinalClassificationReason') AS acquiredsyphilisfinalclassificationreason,
    JSON_EXTRACT_SCALAR(data, '$.syphilis_adquirida[0].acquiredSyphilisObservations') AS acquiredsyphilisobservations,
    PARSE_DATETIME('%b %d, %Y %I:%M:%S %p', JSON_EXTRACT_SCALAR(data, '$.syphilis_adquirida[0].acquiredSyphilisClosureDate')) AS acquiredsyphilisclosuredate,

    SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.syphilis_adquirida[0].acquiredSyphilisAgeAtClosure') AS INT64) AS acquiredsyphilisageatclosure,
    JSON_EXTRACT_SCALAR(data, '$.syphilis_adquirida[0].acquiredSyphilisClosureReason') AS acquiredsyphilisclosurereason,

    loaded_at,
    data_particao
  FROM bruto_atendimento
),

sifilis_extraida AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY id_prontuario_global
      ORDER BY loaded_at DESC
    ) AS rn
  FROM base_sifilis_extraida
)

SELECT * EXCEPT (rn)
FROM sifilis_extraida
WHERE rn = 1