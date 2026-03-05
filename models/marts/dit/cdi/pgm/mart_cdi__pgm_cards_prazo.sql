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

  CASE
    WHEN data_de_entrada > CURRENT_DATE() THEN NULL

    WHEN data_saida_pgm > CURRENT_DATE() THEN NULL

    WHEN data_saida_pgm < data_de_entrada THEN NULL

    ELSE DATE_DIFF(
      data_saida_pgm,
      data_de_entrada,
      DAY
    )
  END AS tempo_atendimento_dias,
  -- Campos para filtros
  COALESCE(TRIM(cap), 'Não informado') AS cap,
  COALESCE(TRIM(origem), 'Não informado') AS origem,
  COALESCE(TRIM(sintese_solicitacao), 'Não informado') AS sintese_solicitacao,
  COALESCE(TRIM(mandado_de_prisao), 'Não informado') AS mandado_de_prisao,
  COALESCE(TRIM(crime_de_desobediencia), 'Não informado') AS crime_de_desobediencia,
  COALESCE(TRIM(setor_responsavel), 'Não informado') AS setor_responsavel

FROM {{ ref('int_cdi__pgm') }}
WHERE 
  UPPER(TRIM(situacao)) LIKE '%RESOLVID%'
  AND data_de_entrada IS NOT NULL
  AND data_saida_pgm IS NOT NULL
  AND SAFE_CAST(prazo_dias AS INT64) IS NOT NULL