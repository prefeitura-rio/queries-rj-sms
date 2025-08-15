{{ config(
    alias="teste_rapido",
    materialized="incremental",
    schema="brutos_prontuario_vitacare_api",
    incremental_strategy="insert_overwrite",
    partition_by={"field": "data_particao", "data_type": "date", "granularity": "day"}
) }}

{% set last_partition = get_last_partition_date(this) %}

WITH bruto_atendimento AS (
  SELECT
    CAST(source_id AS STRING) AS id_prontuario_local,
    CONCAT(NULLIF(CAST(payload_cnes AS STRING), ''), '.', NULLIF(CAST(source_id AS STRING), '')) AS id_prontuario_global,
    CAST(payload_cnes AS STRING) AS id_cnes,
    SAFE_CAST(datalake_loaded_at AS DATETIME) AS loaded_at,
    data,
    DATE(SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.datahora_fim_atendimento') AS DATETIME)) AS data_particao
  FROM {{ source("brutos_prontuario_vitacare_staging", "atendimento_continuo") }}
  {% if is_incremental() %}
    WHERE DATE(loaded_at, 'America/Sao_Paulo') >= DATE('{{ last_partition }}')
  {% endif %}
  qualify row_number() over(partition by id_prontuario_global order by datalake_loaded_at desc) = 1
),

teste_rapido_flat AS (
  SELECT
    id_prontuario_global,
    id_prontuario_local,
    id_cnes,

    JSON_EXTRACT_SCALAR(data, '$.teste_rapido[0].pregnancyTestResult') AS pregnancytestresult,
    JSON_EXTRACT_SCALAR(data, '$.teste_rapido[0].positivePregnancyTestResult') AS positivepregnancytestresult,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.teste_rapido[0].fastingGlucose') AS STRING) AS fastingglucose,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.teste_rapido[0].postprandialGlucose') AS STRING) AS postprandialglucose,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.teste_rapido[0].capillaryGlucose') AS STRING) AS capillaryglucose,
    JSON_EXTRACT_SCALAR(data, '$.teste_rapido[0].syphilisTestResult') AS syphilistestresult,
    JSON_EXTRACT_SCALAR(data, '$.teste_rapido[0].positiveSyphilisTestResult') AS positivesyphilistestresult,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.teste_rapido[0].ppdResult') AS STRING) AS ppdresult,
    JSON_EXTRACT_SCALAR(data, '$.teste_rapido[0].ppdTestDate') AS ppdtestdate,
    JSON_EXTRACT_SCALAR(data, '$.teste_rapido[0].hepatitisCTestResult') AS hepatitisctestresult,
    JSON_EXTRACT_SCALAR(data, '$.teste_rapido[0].positiveHepatitisCTestResult') AS positivehepatitisctestresult,
    JSON_EXTRACT_SCALAR(data, '$.teste_rapido[0].tuberculosisMolecularTestResult') AS tuberculosismoleculartestresult,
    JSON_EXTRACT_SCALAR(data, '$.teste_rapido[0].hepatitisBTestResult') AS hepatitisbtestresult,
    JSON_EXTRACT_SCALAR(data, '$.teste_rapido[0].positiveHepatitisBTestResult') AS positivehepatitisbtestresult,
    JSON_EXTRACT_SCALAR(data, '$.teste_rapido[0].sarsCov2TestResult') AS sarscov2testresult,
    JSON_EXTRACT_SCALAR(data, '$.teste_rapido[0].positiveSarsCov2TestResult') AS positivesarscov2testresult,

    loaded_at,
    data_particao
  FROM bruto_atendimento
),

teste_rapido_dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id_prontuario_global ORDER BY loaded_at DESC) AS rn
  FROM teste_rapido_flat
)

SELECT * EXCEPT (rn)
FROM teste_rapido_dedup
WHERE rn = 1