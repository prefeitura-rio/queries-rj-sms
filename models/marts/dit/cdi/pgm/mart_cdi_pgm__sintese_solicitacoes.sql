{{ config(
  schema = "projeto_cdi",
  alias  = "pgm_sintese_solicitacoes",
  materialized = "table",
  meta={"owner": "karen"}
) }}

SELECT
  COALESCE(TRIM(sintese_solicitacao), 'Não informado') AS sintese_solicitacao,
  COUNT(*) AS total_solicitacoes,

  SAFE_CAST(data_entrada AS DATE) AS data_entrada,
  COALESCE(TRIM(situacao), 'Não informado') AS situacao,
  COALESCE(TRIM(cap), 'Não informado') AS cap,
  COALESCE(TRIM(origem), 'Não informado') AS origem,
  COALESCE(TRIM(mandado_prisao), 'Não informado') AS mandado_prisao,
  COALESCE(TRIM(crime_desobediencia), 'Não informado') AS crime_desobediencia,
  COALESCE(TRIM(setor_responsavel), 'Não informado') AS setor_responsavel

FROM {{ ref('int_cdi__pgm') }}
GROUP BY sintese_solicitacao, data_entrada, situacao, cap, origem, mandado_prisao, crime_desobediencia, setor_responsavel