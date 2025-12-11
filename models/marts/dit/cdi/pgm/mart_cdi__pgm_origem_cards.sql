{{ config(
  schema = "projeto_cdi",
  alias  = "pgm_origem_cards",
  materialized = "table"
) }}

WITH base AS (
  SELECT
    SAFE_CAST(data_de_entrada AS DATE) AS data_de_entrada,
    SAFE_CAST(processorio AS STRING) AS processorio,
    TRIM(situacao) AS situacao,
    TRIM(cap) AS cap,
    TRIM(origem) AS origem,
    TRIM(sintese_solicitacao) AS sintese_solicitacao,
    TRIM(mandado_de_prisao) AS mandado_de_prisao,
    TRIM(crime_de_desobediencia) AS crime_de_desobediencia,
    TRIM(setor_responsavel) AS setor_responsavel,
    TRIM(idade) AS idade,
    TRIM(sexo) AS sexo
  FROM {{ ref('int_cdi__pgm') }}
  WHERE data_de_entrada IS NOT NULL
)

SELECT
  data_de_entrada,
  COALESCE(situacao, 'Não informado')               AS situacao,
  COALESCE(cap, 'Não informado')                    AS cap,
  COALESCE(origem, 'Não informado')                 AS origem,
  COALESCE(sintese_solicitacao, 'Não informado')    AS sintese_solicitacao,
  COALESCE(mandado_de_prisao, 'Não informado')      AS mandado_de_prisao,
  COALESCE(crime_de_desobediencia, 'Não informado') AS crime_de_desobediencia,
  COALESCE(setor_responsavel, 'Não informado')      AS setor_responsavel,
  COALESCE(idade, 'Não informado')                  AS idade,
  COALESCE(sexo, 'Não informado')                   AS sexo,
  processorio
FROM base