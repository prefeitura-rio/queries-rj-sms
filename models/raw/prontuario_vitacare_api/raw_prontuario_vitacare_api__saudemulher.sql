{{ config(
    alias="saude_mulher",
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
  WHERE JSON_EXTRACT(data, '$.saude_mulher') IS NOT NULL
  AND JSON_EXTRACT(data, '$.saude_mulher') != '[]'
  {% if is_incremental() %}
    AND DATE(datalake_loaded_at, 'America/Sao_Paulo') >= DATE('{{ last_partition }}')
  {% endif %}
  qualify row_number() over(partition by id_prontuario_global order by datalake_loaded_at desc) = 1
),

saude_mulher_extraida AS (
  SELECT
    id_prontuario_global,
    id_prontuario_local,
    id_cnes,

    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.obsPf') AS obs_pf,
    
    (
      SELECT JSON_EXTRACT_SCALAR(elem, '$')
      FROM UNNEST(COALESCE(JSON_EXTRACT_ARRAY(data, '$.saude_mulher.sterilizationMethods'), [])) AS elem
      LIMIT 1
    ) AS sterilization_methods,

    (
      SELECT JSON_EXTRACT_SCALAR(elem, '$')
      FROM UNNEST(COALESCE(JSON_EXTRACT_ARRAY(data, '$.saude_mulher.educationActions'), [])) AS elem
      LIMIT 1
    ) AS education_actions,

    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.diudtainiMcont') AS diudtainim_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.diudtatermomCont') AS diudtatermom_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.diuinterrmCont') AS diuinterrm_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.diucopmCont') AS diucopm_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.chocqualMcont') AS chocqualm_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.imqualMcont') AS imqualm_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.itqualMcont') AS itqualm_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.mpqualMcont') AS mpqualm_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.oqualMcont') AS oqualm_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.pmdtainiMcont') AS pmdtainim_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.pfdtainiMcont') AS pfdtainim_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.chocdtainiMcont') AS chocdtainim_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.imdtainiMcont') AS imdtainim_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.itdtainiMcont') AS itdtainim_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.mpdtainiMcont') AS mpdtainim_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.dgdtainiMcont') AS dgdtainim_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.espdtainiMcont') AS espdtainim_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.emdtainiMcont') AS emdtainim_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.efdtainiMcont') AS efdtainim_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.oddtainiMcont') AS odtainim_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.pmdtatermomCont') AS pmdtatermom_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.pfdtatermomCont') AS pfdtatermom_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.chocdtatermomCont') AS chocdtatermom_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.imdtatermomCont') AS imdtatermom_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.itdtatermomCont') AS itdtatermom_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.mpdtatermomCont') AS mpdtatermom_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.dgdtatermomCont') AS dgdtatermom_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.espdtatermomCont') AS espdtatermom_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.emdtatermomCont') AS emdtatermom_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.efdtatermomCont') AS efdtatermom_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.oddtatermomCont') AS oddtatermom_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.pminterrmCont') AS pminterrm_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.pfinterrmCont') AS pfinterrm_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.chocinterrmCont') AS chocinterrm_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.iminterrmCont') AS iminterrm_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.itinterrmCont') AS itinterrm_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.mpinterrmCont') AS mpinterrm_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.dginterrmCont') AS dginterrm_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.espinterrmCont') AS espinterrm_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.eminterrmCont') AS eminterrm_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.efinterrmCont') AS efinterrm_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.ointerrmCont') AS ointerrm_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.pmcompmCont') AS pmcompm_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.pfcompmCont') AS pfcompm_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.choccompmCont') AS choccompm_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.icompmCont') AS icompm_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.itcompmCont') AS itcompm_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.mpcompmCont') AS mpcompm_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.dgcompmCont') AS dgcompm_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.espcompmCont') AS espcompm_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.emcompmCont') AS emcompm_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.efcompmCont') AS efcompm_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.ocompmCont') AS ocompm_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.diuqualMcont') AS diuqualm_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.cidtainiMcont') AS cidtainim_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.cidtatermomCont') AS cidtatermom_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.ciinterrmCont') AS ciinterrm_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.cicompmCont') AS cicompm_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.tabdtainiMcont') AS tabdtainim_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.tabdtatermomCont') AS tabdtatermom_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.tabinterrmCont') AS tabinterrm_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.tabcompmCont') AS tabcompm_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.mcontracepcaoMcont') AS mcontracepcao_mcont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.mbdtainiMcont') AS mbdtainim_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.mbdtatermomCont') AS mbdtatermom_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.mbinterrmCont') AS mbinterrm_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.mbcompmCont') AS mbcompm_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.cedtainiMcont') AS cedtainim_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.cedtatermomCont') AS cedtatermom_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.ceinterrmCont') AS ceinterrm_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.cecompmCont') AS cecompm_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.avdtainiMcont') AS avdtainim_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.avdtatermomCont') AS avdtatermom_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.avinterrmCont') AS avinterrm_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.avcompmCont') AS avcompm_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.planondtainiMcont') AS planondtainim_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.planondtatermomCont') AS planondtatermom_cont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.planonmotivoMcont') AS planonmotivo_mcont,
    JSON_EXTRACT_SCALAR(data, '$.saude_mulher.planoncomplicacoesMcont') AS planoncomplicacoes_mcont,

    loaded_at,
    data_particao

  FROM bruto_atendimento
),

saude_mulher_dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id_prontuario_global ORDER BY loaded_at DESC) AS rn
  FROM saude_mulher_extraida
)

SELECT * EXCEPT (rn)
FROM saude_mulher_dedup
WHERE rn = 1