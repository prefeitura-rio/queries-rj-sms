{{ config(
  schema = "projeto_cdi",
  alias  = "pgm_origem_cards",
  materialized = "table",
  meta={"owner": "karen"}
) }}

WITH base AS (
  SELECT
    SAFE_CAST(data_entrada AS DATE) AS data_entrada,
    SAFE_CAST(processo_rio AS STRING) AS processo_rio,
    TRIM(situacao) AS situacao,
    TRIM(cap) AS cap,
    TRIM(origem) AS origem,
    TRIM(sintese_solicitacao) AS sintese_solicitacao,
    TRIM(mandado_prisao) AS mandado_prisao,
    TRIM(crime_desobediencia) AS crime_desobediencia,
    TRIM(setor_responsavel) AS setor_responsavel,
    TRIM(idade) AS idade,
    TRIM(sexo) AS sexo
  FROM {{ ref('int_cdi__pgm') }}
  WHERE data_entrada IS NOT NULL
)

SELECT
  data_entrada,
  COALESCE(situacao, 'Não informado')               AS situacao,
  COALESCE(cap, 'Não informado')                    AS cap,
  COALESCE(origem, 'Não informado')                 AS origem,
  COALESCE(sintese_solicitacao, 'Não informado')    AS sintese_solicitacao,
  COALESCE(mandado_prisao, 'Não informado')         AS mandado_prisao,
  COALESCE(crime_desobediencia, 'Não informado')    AS crime_desobediencia,
  COALESCE(setor_responsavel, 'Não informado')      AS setor_responsavel,
  COALESCE(idade, 'Não informado')                  AS idade,
  COALESCE(sexo, 'Não informado')                   AS sexo,
  processo_rio
FROM base