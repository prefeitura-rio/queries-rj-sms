{{ config(
    alias="teste_rapido",
    materialized="incremental",
    schema="brutos_prontuario_vitacare_api",
    incremental_strategy="insert_overwrite",
    partition_by={"field": "data_particao", "data_type": "date", "granularity": "day"}
) }}

{% set last_30_days = (
    "date_sub(current_date('America/Sao_Paulo'), interval 30 day)"
) %}

WITH bruto_atendimento AS (
  SELECT
    CAST(source_id AS STRING) AS id_prontuario_local,
    CONCAT(NULLIF(CAST(payload_cnes AS STRING), ''), '.', NULLIF(CAST(source_id AS STRING), '')) AS id_prontuario_global,
    CAST(payload_cnes AS STRING) AS id_cnes,
    SAFE_CAST(datalake_loaded_at AS DATETIME) AS loaded_at,
    data,
    DATE(SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.datahora_fim_atendimento') AS DATETIME)) AS data_particao
  FROM {{ source("brutos_prontuario_vitacare_api_staging", "atendimento_continuo") }}
  WHERE JSON_EXTRACT(data, '$.teste_rapido') IS NOT NULL
  AND JSON_EXTRACT(data, '$.teste_rapido') != '[]'
  {% if is_incremental() %}
    AND DATE(datalake_loaded_at, 'America/Sao_Paulo') >= {{ last_30_days }}
  {% endif %}
  qualify row_number() over(partition by id_prontuario_global order by datalake_loaded_at desc) = 1
),

teste_rapido_flat AS (
  SELECT
    id_prontuario_global,
    id_prontuario_local,
    id_cnes,

    {{ process_null("JSON_EXTRACT_SCALAR(data, '$.teste_rapido[0].pregnancyTestResult')") }} AS pregnancytestresult,
    {{ process_null("JSON_EXTRACT_SCALAR(data, '$.teste_rapido[0].positivePregnancyTestResult')") }} AS positivepregnancytestresult,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.teste_rapido[0].fastingGlucose') AS STRING) AS fastingglucose,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.teste_rapido[0].postprandialGlucose') AS STRING) AS postprandialglucose,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.teste_rapido[0].capillaryGlucose') AS STRING) AS capillaryglucose,
    {{ process_null("JSON_EXTRACT_SCALAR(data, '$.teste_rapido[0].syphilisTestResult')") }} AS syphilistestresult,
    {{ process_null("JSON_EXTRACT_SCALAR(data, '$.teste_rapido[0].positiveSyphilisTestResult')") }} AS positivesyphilistestresult,
    {{ process_null("JSON_EXTRACT_SCALAR(data, '$.teste_rapido[0].ppdResult')") }} AS ppdresult,
    JSON_EXTRACT_SCALAR(data, '$.teste_rapido[0].ppdTestDate') AS ppdtestdate,
    {{ process_null("JSON_EXTRACT_SCALAR(data, '$.teste_rapido[0].hepatitisCTestResult')") }} AS hepatitisctestresult,
    {{ process_null("JSON_EXTRACT_SCALAR(data, '$.teste_rapido[0].positiveHepatitisCTestResult')") }} AS positivehepatitisctestresult,
    {{ process_null("JSON_EXTRACT_SCALAR(data, '$.teste_rapido[0].tuberculosisMolecularTestResult')") }} AS tuberculosismoleculartestresult,
    {{ process_null("JSON_EXTRACT_SCALAR(data, '$.teste_rapido[0].hepatitisBTestResult')") }} AS hepatitisbtestresult,
    {{ process_null("JSON_EXTRACT_SCALAR(data, '$.teste_rapido[0].positiveHepatitisBTestResult')") }} AS positivehepatitisbtestresult,
    {{ process_null("JSON_EXTRACT_SCALAR(data, '$.teste_rapido[0].sarsCov2TestResult')") }} AS sarscov2testresult,
    {{ process_null("JSON_EXTRACT_SCALAR(data, '$.teste_rapido[0].positiveSarsCov2TestResult')") }} AS positivesarscov2testresult,
    {{ process_null("JSON_EXTRACT_SCALAR(data, '$.teste_rapido[0].hiv01TestResult')") }} AS hiv01testresult,
    {{ process_null("JSON_EXTRACT_SCALAR(data, '$.teste_rapido[0].hiv02TestResult')") }} AS hiv02testresult,
    {{ process_null("JSON_EXTRACT_SCALAR(data, '$.teste_rapido[0].antihiv_gestanteTestResult')") }} AS antihiv_gestantetestresult,
    {{ process_null("JSON_EXTRACT_SCALAR(data, '$.teste_rapido[0].antihiv_parceiroTestResult')") }} AS antihiv_parceirotestresult,
    {{ process_null("JSON_EXTRACT_SCALAR(data, '$.teste_rapido[0].hepb_gestanteTestResult')") }} AS hepb_gestantetestresult,
    {{ process_null("JSON_EXTRACT_SCALAR(data, '$.teste_rapido[0].hepb_parceiroTestResult')") }} AS hepb_parceirotestresult,
    {{ process_null("JSON_EXTRACT_SCALAR(data, '$.teste_rapido[0].hepb_pop_geralTestResult')") }} AS hepb_pop_geraltestresult,
    {{ process_null("JSON_EXTRACT_SCALAR(data, '$.teste_rapido[0].hepc_gestanteTestResult')") }} AS hepc_gestantetestresult,
    {{ process_null("JSON_EXTRACT_SCALAR(data, '$.teste_rapido[0].hepc_parceiroTestResult')") }} AS hepc_parceirotestresult,
    {{ process_null("JSON_EXTRACT_SCALAR(data, '$.teste_rapido[0].hepc_pop_geralTestResult')") }} AS hepc_pop_geraltestresult,
    {{ process_null("JSON_EXTRACT_SCALAR(data, '$.teste_rapido[0].treponemico_sifilis_gestanteTestResult')") }} AS treponemico_sifilis_gestantetestresult,
    {{ process_null("JSON_EXTRACT_SCALAR(data, '$.teste_rapido[0].treponemico_sifilis_parceiroTestResult')") }} AS treponemico_sifilis_parceirotestresult,
    {{ process_null("JSON_EXTRACT_SCALAR(data, '$.teste_rapido[0].proteinuriaTestResult')") }} AS proteinuriatestresult,
    {{ process_null("JSON_EXTRACT_SCALAR(data, '$.teste_rapido[0].tbTestResult')") }} AS tbtestresult,

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