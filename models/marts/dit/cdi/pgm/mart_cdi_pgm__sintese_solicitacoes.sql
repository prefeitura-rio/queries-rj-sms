{{ config(
  schema = "projeto_cdi",
  alias  = "pgm_sintese_solicitacoes",
  materialized = "table"
) }}

SELECT
  COALESCE(TRIM(sintese_solicitacao), 'Não informado') AS sintese_solicitacao,
  COUNT(*) AS total_solicitacoes,

  -- Campos de filtro
  SAFE_CAST(data_de_entrada AS DATE) AS data_de_entrada,
  COALESCE(TRIM(situacao), 'Não informado') AS situacao,
  COALESCE(TRIM(cap), 'Não informado') AS cap,
  COALESCE(TRIM(origem), 'Não informado') AS origem,
  COALESCE(TRIM(mandado_de_prisao), 'Não informado') AS mandado_de_prisao,
  COALESCE(TRIM(crime_de_desobediencia), 'Não informado') AS crime_de_desobediencia,
  COALESCE(TRIM(setor_responsavel), 'Não informado') AS setor_responsavel

FROM {{ ref('int_cdi__pgm') }}
GROUP BY sintese_solicitacao, data_de_entrada, situacao, cap, origem, mandado_de_prisao, crime_de_desobediencia, setor_responsavel