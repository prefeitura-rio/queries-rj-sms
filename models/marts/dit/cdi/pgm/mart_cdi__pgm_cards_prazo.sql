{{ config(
  schema = "projeto_cdi",
  alias  = "pgm_cards_prazo",
  materialized = "table"
) }}

SELECT
  SAFE_CAST(processorio AS STRING) AS processorio,
  SAFE_CAST(data_de_entrada AS DATE) AS data_de_entrada,
  SAFE_CAST(data_saida_pgm AS DATE) AS data_saida_pgm,
  SAFE_CAST(prazo_dias AS INT64) AS prazo_dias,

  -- Cálculo do prazo limite
  DATE_ADD(SAFE_CAST(data_de_entrada AS DATE), INTERVAL SAFE_CAST(prazo_dias AS INT64) DAY) AS prazo_limite,

  -- Situação e status
  UPPER(TRIM(situacao)) AS situacao,
  CASE
    WHEN SAFE_CAST(data_saida_pgm AS DATE) <= DATE_ADD(SAFE_CAST(data_de_entrada AS DATE), INTERVAL SAFE_CAST(prazo_dias AS INT64) DAY)
      THEN 'Dentro do prazo'
    ELSE 'Fora do prazo'
  END AS status_prazo,

  -- Métrica de tempo de atendimento
  DATE_DIFF(SAFE_CAST(data_saida_pgm AS DATE), SAFE_CAST(data_de_entrada AS DATE), DAY) AS tempo_atendimento_dias,

  -- Campos para filtros
  COALESCE(TRIM(cap), 'Não informado') AS cap,
  COALESCE(TRIM(origem), 'Não informado') AS origem,
  COALESCE(TRIM(sintese_solicitacao), 'Não informado') AS sintese_solicitacao,
  COALESCE(TRIM(mandado_de_prisao), 'Não informado') AS mandado_de_prisao,
  COALESCE(TRIM(crime_de_desobediencia), 'Não informado') AS crime_de_desobediencia,
  COALESCE(TRIM(setor_responsavel), 'Não informado') AS setor_responsavel

FROM {{ ref('int_cdi_pgm') }}
WHERE 
  UPPER(TRIM(situacao)) LIKE '%RESOLVID%'
  AND data_de_entrada IS NOT NULL
  AND data_saida_pgm IS NOT NULL
  AND SAFE_CAST(prazo_dias AS INT64) IS NOT NULL