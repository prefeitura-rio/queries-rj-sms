{{ config(
    alias="saude_bucal",
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
    safe_cast(json_extract_scalar(data, '$.datahora_fim_atendimento') as datetime) as datahora_fim_atendimento
  FROM {{ source('brutos_prontuario_vitacare_staging', 'atendimento_continuo') }}
  {% if is_incremental() %}
    WHERE DATE(loaded_at, 'America/Sao_Paulo') >= DATE('{{ last_partition }}')
  {% endif %}
  qualify row_number() over(partition by id_prontuario_global order by datalake_loaded_at desc) = 1
),

saude_bucal_extracted AS (
  SELECT
    id_prontuario_global,
    id_prontuario_local,
    id_cnes,

    JSON_EXTRACT_SCALAR(data, '$.saude_bucal[0].mainComplaint') AS queixa_principal,
    JSON_EXTRACT_SCALAR(data, '$.saude_bucal[0].mucosalLesions') AS lesoes_na_mucosa,
    JSON_EXTRACT_SCALAR(data, '$.saude_bucal[0].mucosalLesionsDescription') AS lesoes_na_mucosa_descricao,
    JSON_EXTRACT_SCALAR(data, '$.saude_bucal[0].fluorose') AS fluorose,
    JSON_EXTRACT_SCALAR(data, '$.saude_bucal[0].enamelAlterations') AS alteracoes_do_esmalte,
    JSON_EXTRACT_SCALAR(data, '$.saude_bucal[0].tmj') AS articulacao_temporomandibular,
    JSON_EXTRACT_SCALAR(data, '$.saude_bucal[0].occlusion') AS oclusao,
    JSON_EXTRACT_SCALAR(data, '$.saude_bucal[0].congenitalAnomaly') AS anomalia_congenita,
    JSON_EXTRACT_SCALAR(data, '$.saude_bucal[0].habitosPrejudiciais') AS habitos_prejudiciais,
    JSON_EXTRACT_SCALAR(data, '$.saude_bucal[0].necessidadeDeProtese') AS necessidade_de_protese,
    JSON_EXTRACT_SCALAR(data, '$.saude_bucal[0].cariesActivity') AS atividade_de_carie,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.saude_bucal[0].lastDentalVisit') AS DATETIME) AS ultima_visita_ao_dentista,
    JSON_EXTRACT_SCALAR(data, '$.saude_bucal[0].clinicalAssessmentObservations') AS observacoes_da_avaliacao_clinica,
    JSON_EXTRACT_SCALAR(data, '$.saude_bucal[0].demandType') AS tipo_de_demanda,
    JSON_EXTRACT_SCALAR(data, '$.saude_bucal[0].oralHealthSurveillance') AS vigilancia_em_saude_bucal,
    JSON_EXTRACT_SCALAR(data, '$.saude_bucal[0].treatmentType') AS tipo_de_tratamento,
    JSON_EXTRACT_SCALAR(data, '$.saude_bucal[0].individualSuppliesProvided') AS suprimentos_individuais_fornecidos,
    JSON_EXTRACT_SCALAR(data, '$.saude_bucal[0].treatmentDischarge') AS alta_do_tratamento,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.saude_bucal[0].dischargeDate') AS DATETIME) AS data_da_alta,
    JSON_EXTRACT_SCALAR(data, '$.saude_bucal[0].dischargeType') AS tipo_de_alta,
    JSON_EXTRACT_SCALAR(data, '$.saude_bucal[0].numberOfTeethInTreatment') AS numero_de_dentes_em_tratamento,
    JSON_EXTRACT_SCALAR(data, '$.saude_bucal[0].periodontalDiseaseActivity') AS atividade_de_doenca_periodontal,
    JSON_EXTRACT_SCALAR(data, '$.saude_bucal[0].assessment') AS avaliacao,
    JSON_EXTRACT_SCALAR(data, '$.saude_bucal[0].assessment.description') AS observacoes_da_avaliacao,
    JSON_EXTRACT_SCALAR(data, '$.saude_bucal[0].dentalProcedures') AS procedimentos_realizados,
    JSON_EXTRACT_SCALAR(data, '$.saude_bucal[0].assessment_1716') AS avaliacao_seguinte_1716,
    JSON_EXTRACT_SCALAR(data, '$.saude_bucal[0].assessment_11') AS avaliacao_seguinte_11,
    JSON_EXTRACT_SCALAR(data, '$.saude_bucal[0].assessment_2627') AS avaliacao_seguinte_2627,
    JSON_EXTRACT_SCALAR(data, '$.saude_bucal[0].assessment_3637') AS avaliacao_seguinte_3637,
    JSON_EXTRACT_SCALAR(data, '$.saude_bucal[0].assessment_31') AS avaliacao_seguinte_31,
    JSON_EXTRACT_SCALAR(data, '$.saude_bucal[0].assessment_4746') AS avaliacao_seguinte_4746,
    JSON_EXTRACT_SCALAR(data, '$.saude_bucal[0].useOfDentalProsthesis') AS uso_de_protese_dentaria,
    JSON_EXTRACT_SCALAR(data, '$.saude_bucal[0].patientReferrals') AS encaminhamento_especialidade,

    loaded_at,
    date(datahora_fim_atendimento) as data_particao
  FROM bruto_atendimento
)

SELECT * FROM saude_bucal_extracted