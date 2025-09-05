{{ config(
    alias="hiv",
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
    CONCAT(
      NULLIF(CAST(payload_cnes AS STRING), ''), 
      '.', 
      NULLIF(CAST(source_id AS STRING), '')
    ) AS id_prontuario_global,
    CAST(payload_cnes AS STRING) AS id_cnes,
    SAFE_CAST(datalake_loaded_at AS DATETIME) AS loaded_at,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.datahora_fim_atendimento') AS DATETIME) AS datahora_fim_atendimento,
    data,
  FROM {{ source("brutos_prontuario_vitacare_staging_dev", "atendimento_continuo") }}
  WHERE JSON_EXTRACT(data, '$.hiv') IS NOT NULL
  AND JSON_EXTRACT(data, '$.hiv') != '[]'
  {% if is_incremental() %}
    AND DATE(datalake_loaded_at, 'America/Sao_Paulo') >= {{ last_30_days }}
  {% endif %}
  qualify row_number() over (partition by id_prontuario_global order by loaded_at desc) = 1
),

hiv_extracted_base AS (
  SELECT
    id_prontuario_global,
    id_prontuario_local,
    id_cnes,
    
    JSON_EXTRACT_SCALAR(data, '$.hiv[0].aidsTransfusao') AS aidstransfusao,
    JSON_EXTRACT_SCALAR(data, '$.hiv[0].aidsDataTransfusao') AS aidsdatatransfusao,
    JSON_EXTRACT_SCALAR(data, '$.hiv[0].aidsIdadeTransfusao') AS aidsidadetransfusao,
    JSON_EXTRACT_SCALAR(data, '$.hiv[0].aidsToxicoDependente') AS aidstoxicodependente,

    CAST(NULL AS DATE) AS aidsdatainiciotoxicodependente,
    CAST(NULL AS INT64) AS aidsidadeiniciotoxicodependente,

    JSON_EXTRACT_SCALAR(data, '$.hiv[0].aidsHomosexual') AS aidshomosexual,
    JSON_EXTRACT_SCALAR(data, '$.hiv[0].aidsDataInicioHomosexual') AS aidsdatainiciohomosexual,
    JSON_EXTRACT_SCALAR(data, '$.hiv[0].aidsIdadeInicioHomosexual') AS aidsidadeiniciohomosexual,
    JSON_EXTRACT_SCALAR(data, '$.hiv[0].aidsHeterosexual') AS aidsheterosexual,
    JSON_EXTRACT_SCALAR(data, '$.hiv[0].aidsDataInicioHeterosexual') AS aidsdatainicioheterosexual,
    JSON_EXTRACT_SCALAR(data, '$.hiv[0].aidsIdadeInicioHeterosexual') AS aidsidadeinicioheterosexual,
    JSON_EXTRACT_SCALAR(data, '$.hiv[0].aidsBisexual') AS aidsbisexual,

    CAST(NULL AS DATE) AS aidsdatainiciobisexual,
    CAST(NULL AS INT64) AS aidsidadeiniciobisexual,

    JSON_EXTRACT_SCALAR(data, '$.hiv[0].aidsParceiroVih') AS aidsparceirovih,
    JSON_EXTRACT_SCALAR(data, '$.hiv[0].aidsDataInicioParceiroVih') AS aidsdatainicioparceirovih,
    JSON_EXTRACT_SCALAR(data, '$.hiv[0].aidsIdadeInicioParceiroVih') AS aidsidadeinicioparceirovih,
    JSON_EXTRACT_SCALAR(data, '$.hiv[0].aidsObsEpidm') AS aidsobsepidm,
    JSON_EXTRACT_SCALAR(data, '$.hiv[0].aidsDoencaAtualAno1Teste') AS aidsdoencaatualano1teste,
    JSON_EXTRACT_SCALAR(data, '$.hiv[0].aidsDoencaAtualDtaProvavelInfec') AS aidsdoencaatualdtaprovavelinfec,
    JSON_EXTRACT_SCALAR(data, '$.hiv[0].aidsDoencaAtualFebre') AS aidsdoencaatualfebre,
    JSON_EXTRACT_SCALAR(data, '$.hiv[0].aidsDoencaAtualAstenia') AS aidsdoencaatualastenia,
    JSON_EXTRACT_SCALAR(data, '$.hiv[0].aidsDoencaAtualAnorexia') AS aidsdoencaatualanorexia,
    JSON_EXTRACT_SCALAR(data, '$.hiv[0].aidsDoencaAtualEmagrecimento') AS aidsdoencaatualemagrecimento,
    JSON_EXTRACT_SCALAR(data, '$.hiv[0].aidsDoencaAtualHiperSudorese') AS aidsdoencaatualhipersudorese,
    JSON_EXTRACT_SCALAR(data, '$.hiv[0].aidsDoencaAtualErupcaoCutanea') AS aidsdoencaatualerupcaocutanea,
    JSON_EXTRACT_SCALAR(data, '$.hiv[0].aidsDoencaAtualAltPele') AS aidsdoencaatualaltpele,
    JSON_EXTRACT_SCALAR(data, '$.hiv[0].aidsDoencaAtualAltMucosa') AS aidsdoencaatualaltmucosa,
    JSON_EXTRACT_SCALAR(data, '$.hiv[0].aidsDoencaAtualAdenopatias') AS aidsdoencaatualadenopatias,
    JSON_EXTRACT_SCALAR(data, '$.hiv[0].aidsDoencaAtualAdenoTamanho') AS aidsdoencaatualadenotamanho,
    JSON_EXTRACT_SCALAR(data, '$.hiv[0].aidsDoencaAtualAdenoDolorosas') AS aidsdoencaatualadenodolorosas,
    JSON_EXTRACT_SCALAR(data, '$.hiv[0].aidsDoencaAtualVomitos') AS aidsdoencaatualvomitos,
    JSON_EXTRACT_SCALAR(data, '$.hiv[0].aidsDoencaAtualDiarreia') AS aidsdoencaatualdiarreia,
    JSON_EXTRACT_SCALAR(data, '$.hiv[0].aidsDoencaAtualDisfagia') AS aidsdoencaatualdisfagia,
    JSON_EXTRACT_SCALAR(data, '$.hiv[0].aidsDoencaAtualDorAbdominal') AS aidsdoencaatualdorabdominal,
    JSON_EXTRACT_SCALAR(data, '$.hiv[0].aidsDoencaAtualTosse') AS aidsdoencaatualtosse,
    JSON_EXTRACT_SCALAR(data, '$.hiv[0].aidsDoencaAtualExpectoracao') AS aidsdoencaatualexpectoracao,
    JSON_EXTRACT_SCALAR(data, '$.hiv[0].aidsDoencaAtualDispneia') AS aidsdoencaatualdispneia,
    JSON_EXTRACT_SCALAR(data, '$.hiv[0].aidsDoencaAtualDorToraxica') AS aidsdoencaatualdortoraxica,
    JSON_EXTRACT_SCALAR(data, '$.hiv[0].aidsDoencaAtualCefaleias') AS aidsdoencaatualcefaleias,
    JSON_EXTRACT_SCALAR(data, '$.hiv[0].aidsDoencaAtualPertVisuais') AS aidsdoencaatualpertvisuais,
    JSON_EXTRACT_SCALAR(data, '$.hiv[0].aidsDoencaAtualAltComportamento') AS aidsdoencaatualaltcomportamento,
    JSON_EXTRACT_SCALAR(data, '$.hiv[0].aidsDoencaAtualVertigens') AS aidsdoencaatualvertigens,
    JSON_EXTRACT_SCALAR(data, '$.hiv[0].aidsDoencaAtualAltEsfincterianas') AS aidsdoencaatualaltesfincterianas,
    JSON_EXTRACT_SCALAR(data, '$.hiv[0].aidsDoencaAtualAltSensibilidade') AS aidsdoencaatualaltsensibilidade,
    JSON_EXTRACT_SCALAR(data, '$.hiv[0].aidsDoencaAtualAltMotorasSup') AS aidsdoencaatualaltmotorassup,
    JSON_EXTRACT_SCALAR(data, '$.hiv[0].aidsDoencaAtualAltMotorasInf') AS aidsdoencaatualaltmotorasinf,
    JSON_EXTRACT_SCALAR(data, '$.hiv[0].aidsDoencaAtualDtInicioQueixas') AS aidsdoencaatualdtinicioqueixas,
    JSON_EXTRACT_SCALAR(data, '$.hiv[0].aidsDoencaAtualObs') AS aidsdoencaatualobs,

    JSON_EXTRACT_SCALAR(data, '$.hiv[0].aidsTransfusaoVertical') AS aidstransfusao_vertical,
    CAST(NULL AS DATE) AS aidstransfusao_vertical_data,

    JSON_EXTRACT_SCALAR(data, '$.hiv[0].aidsHemofilico') AS aidshemofilico,
    CAST(NULL AS DATE) AS aidshemofilicodata,

    JSON_EXTRACT_SCALAR(data, '$.hiv[0].aidsAcidenteTrabalho') AS aidsacidentetrabalho,
    CAST(NULL AS DATE) AS aidsacidentetrabalhodata,

    JSON_EXTRACT_SCALAR(data, '$.hiv[0].aidsTempoEmagrecimento') AS aidstempoemagrecimento,
    JSON_EXTRACT_SCALAR(data, '$.hiv[0].epidemiologiaIgnorado') AS epidemiologiaignorado,
    JSON_EXTRACT_SCALAR(data, '$.hiv[0].aidsObsNotas') AS aidsobsnotas,
    JSON_EXTRACT_SCALAR(data, '$.hiv[0].aidsEmTerapiaAntiRetroviral') AS aidsemterapiaantiretroviral,

    loaded_at,
    DATE(datahora_fim_atendimento) AS data_particao
  FROM bruto_atendimento
),

hiv_extracted AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY id_prontuario_global
      ORDER BY loaded_at DESC
    ) AS rn
  FROM hiv_extracted_base
)

SELECT * EXCEPT (rn)
FROM hiv_extracted
WHERE rn = 1
