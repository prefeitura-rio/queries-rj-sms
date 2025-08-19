{{ config(
    alias="hanseniase",
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
      NULLIF(TRIM(CAST(payload_cnes AS STRING)), ''),
      '.',
      NULLIF(TRIM(CAST(source_id AS STRING)), '')
    ) AS id_prontuario_global,
    CAST(payload_cnes AS STRING) AS id_cnes,
    SAFE_CAST(datalake_loaded_at AS DATETIME) AS loaded_at,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.datahora_fim_atendimento') AS DATETIME) AS datahora_fim_atendimento,
    data
  FROM {{ source("brutos_prontuario_vitacare_staging", "atendimento_continuo") }}
  {% if is_incremental() %}
    WHERE DATE(datalake_loaded_at, 'America/Sao_Paulo') >= DATE('{{ last_partition }}')
  {% endif %}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id_prontuario_global ORDER BY loaded_at DESC) = 1
),

hanseniase_extraida AS (
  SELECT
    id_prontuario_global,
    id_prontuario_local,
    id_cnes,

    SAFE_CAST(REGEXP_REPLACE(JSON_EXTRACT_SCALAR(data, '$.hanseniase[0].dataNotificacao'), r' [+-]\d{4}$', '') AS DATETIME) AS data_notificacao,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.hanseniase[0].numLesoesCut') AS STRING) AS num_lesoes_cut,
    JSON_EXTRACT_SCALAR(data, '$.hanseniase[0].formaClinica') AS forma_clinica,
    JSON_EXTRACT_SCALAR(data, '$.hanseniase[0].classificacaoOperacional') AS classificacao_operacional,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.hanseniase[0].numNervosAfetados') AS STRING) AS num_nervos_afetados,
    JSON_EXTRACT_SCALAR(data, '$.hanseniase[0].avaliacaoGrauIncapacidade') AS avaliacao_grau_incapacidade,
    JSON_EXTRACT_SCALAR(data, '$.hanseniase[0].modoEntrada') AS modo_entrada,
    JSON_EXTRACT_SCALAR(data, '$.hanseniase[0].baciloscopia') AS baciloscopia,

    CAST(SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.hanseniase[0].dataExameHistopatologico') AS TIMESTAMP) AS DATETIME) AS data_exame_histopatologico,
    JSON_EXTRACT_SCALAR(data, '$.hanseniase[0].resultadoExameHistopatologico') AS resultado_exame_histopatologico,
    CAST(SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.hanseniase[0].dataInicioTratamento') AS TIMESTAMP) AS DATETIME) AS data_inicio_tratamento,

    SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.hanseniase[0].idadeInicioTratamento') AS STRING) AS idade_inicio_tratamento,
    JSON_EXTRACT_SCALAR(data, '$.hanseniase[0].esquemaTerapeutico') AS esquema_terapeutico,
    JSON_EXTRACT_SCALAR(data, '$.hanseniase[0].historiaClinicaDoencaObservacoes') AS historia_clinica_doenca_observacoes,
    JSON_EXTRACT_SCALAR(data, '$.hanseniase[0].incapacidadeOlho') AS incapacidade_olho,
    JSON_EXTRACT_SCALAR(data, '$.hanseniase[0].incapacidadeMao') AS incapacidade_mao,
    JSON_EXTRACT_SCALAR(data, '$.hanseniase[0].incapacidadePe') AS incapacidade_pe,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.hanseniase[0].maiorGrauAvaliadoTratamento') AS STRING) AS maior_grau_avaliado_tratamento,
    JSON_EXTRACT_SCALAR(data, '$.hanseniase[0].comprometimentoLaringeo') AS comprometimento_laringeo,
    JSON_EXTRACT_SCALAR(data, '$.hanseniase[0].desabamentoNasal') AS desabamento_nasal,
    JSON_EXTRACT_SCALAR(data, '$.hanseniase[0].paralisiaFacial') AS paralisia_facial,
    JSON_EXTRACT_SCALAR(data, '$.hanseniase[0].observacoesNotasGerais') AS observacoes_notas_gerais,
    JSON_EXTRACT_SCALAR(data, '$.hanseniase[0].exameContatosResultado') AS exame_contatos_resultado,
    JSON_EXTRACT_SCALAR(data, '$.hanseniase[0].dadosAcompanhamentoCondicao') AS dados_acompanhamento_condicao,
    JSON_EXTRACT_SCALAR(data, '$.hanseniase[0].dadosAcompanhamentoObservacoes') AS dados_acompanhamento_observacoes,

    CAST(SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.hanseniase[0].dataExclusao') AS TIMESTAMP) AS DATETIME) AS data_exclusao,

    SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.hanseniase[0].idadeNaExclusao') AS STRING) AS idade_na_exclusao,
    JSON_EXTRACT_SCALAR(data, '$.hanseniase[0].motivoExclusao') AS motivo_exclusao,
    JSON_EXTRACT_SCALAR(data, '$.hanseniase[0].numeroSinanHansen') AS numero_sinan_hansen,
    JSON_EXTRACT_SCALAR(data, '$.hanseniase[0].hanseniaseComunicantesReferidos') AS hanseniase_comunicantes_referidos,

    loaded_at,
    DATE(datahora_fim_atendimento) AS data_particao
  FROM bruto_atendimento
)

SELECT *
FROM hanseniase_extraida