{{ config(
  schema = "projeto_cdi",
  alias  = "pgm_proximas_demandas",
  materialized = "table"
) }}

WITH base AS (
  SELECT
    SAFE_CAST(processorio AS STRING) AS processorio,
    COALESCE(TRIM(origem), 'Não informado') AS origem,
    SAFE_CAST(data_de_entrada AS DATE) AS data_de_entrada,
    SAFE_CAST(prazo_dias AS INT64) AS prazo_dias,
    COALESCE(TRIM(situacao), 'Não informado') AS situacao,
    TRIM(cap) AS cap,
    TRIM(sintese_solicitacao) AS sintese_solicitacao,
    TRIM(mandado_de_prisao) AS mandado_de_prisao,
    TRIM(crime_de_desobediencia) AS crime_de_desobediencia,
    TRIM(setor_responsavel) AS setor_responsavel
  FROM {{ ref('int_cdi_pgm') }}
  WHERE prazo_dias IS NOT NULL
),

calc AS (
  SELECT
    processorio,
    origem,
    data_de_entrada,
    DATE_ADD(data_de_entrada, INTERVAL prazo_dias DAY) AS data_vencimento,
    situacao,
    cap,
    sintese_solicitacao,
    mandado_de_prisao,
    crime_de_desobediencia,
    setor_responsavel,
    DATE_DIFF(
      DATE_ADD(data_de_entrada, INTERVAL prazo_dias DAY),
      CURRENT_DATE(),
      DAY
    ) AS dias_restantes
  FROM base
),

calc_legivel AS (
  SELECT
    processorio,
    origem,
    data_de_entrada,
    data_vencimento,
    situacao,
    cap,
    sintese_solicitacao,
    mandado_de_prisao,
    crime_de_desobediencia,
    setor_responsavel,
    dias_restantes,
    CASE
      WHEN data_vencimento < CURRENT_DATE()
        THEN CONCAT('Vencida há ', ABS(dias_restantes), ' dias')
      WHEN data_vencimento = CURRENT_DATE()
        THEN 'Vence hoje'
      ELSE CONCAT('Em ', dias_restantes, ' dias')
    END AS prazo_legivel
  FROM calc
),

filtrado AS (
  SELECT
    *
  FROM calc_legivel
  WHERE UPPER(situacao) LIKE '%PENDENTE%'
)

SELECT
  processorio,
  origem,
  data_de_entrada,
  data_vencimento,
  situacao,
  COALESCE(cap, 'Não informado') AS cap,
  COALESCE(sintese_solicitacao, 'Não informado') AS sintese_solicitacao,
  COALESCE(mandado_de_prisao, 'Não informado') AS mandado_de_prisao,
  COALESCE(crime_de_desobediencia, 'Não informado') AS crime_de_desobediencia,
  COALESCE(setor_responsavel, 'Não informado') AS setor_responsavel,
  prazo_legivel
FROM filtrado
ORDER BY data_vencimento ASC