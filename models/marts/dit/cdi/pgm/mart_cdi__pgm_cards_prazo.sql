{{ config(
  schema = "projeto_cdi",
  alias  = "pgm_cards_prazo",
  materialized = "table",
  meta={"owner": "karen"}
) }}

SELECT
  SAFE_CAST(processo_rio AS STRING) AS processo_rio,
  SAFE_CAST(data_entrada AS DATE) AS data_entrada,
  SAFE_CAST(data_saida_pgm AS DATE) AS data_saida_pgm,
  SAFE_CAST(prazo_dias AS INT64) AS prazo_dias,

  DATE_ADD(SAFE_CAST(data_entrada AS DATE), INTERVAL SAFE_CAST(prazo_dias AS INT64) DAY) AS prazo_limite,

  UPPER(TRIM(situacao)) AS situacao,
  CASE
    WHEN SAFE_CAST(data_saida_pgm AS DATE) <= DATE_ADD(SAFE_CAST(data_entrada AS DATE), INTERVAL SAFE_CAST(prazo_dias AS INT64) DAY)
      THEN 'Dentro do prazo'
    ELSE 'Fora do prazo'
  END AS status_prazo,

  DATE_DIFF(SAFE_CAST(data_saida_pgm AS DATE), SAFE_CAST(data_entrada AS DATE), DAY) AS tempo_atendimento_dias,

  COALESCE(TRIM(cap), 'Não informado') AS cap,
  COALESCE(TRIM(origem), 'Não informado') AS origem,
  COALESCE(TRIM(sintese_solicitacao), 'Não informado') AS sintese_solicitacao,
  COALESCE(TRIM(mandado_prisao), 'Não informado') AS mandado_prisao,
  COALESCE(TRIM(crime_desobediencia), 'Não informado') AS crime_desobediencia,
  COALESCE(TRIM(setor_responsavel), 'Não informado') AS setor_responsavel

FROM {{ ref('int_cdi__pgm') }}
WHERE 
  UPPER(TRIM(situacao)) LIKE '%RESOLVID%'
  AND data_entrada IS NOT NULL
  AND data_saida_pgm IS NOT NULL
  AND SAFE_CAST(prazo_dias AS INT64) IS NOT NULL