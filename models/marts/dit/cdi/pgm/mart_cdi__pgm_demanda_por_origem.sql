{{ config(
  schema = "projeto_cdi",
  alias  = "pgm_demanda_por_origem",
  materialized = "table",
  meta={"owner": "karen"}
) }}

SELECT
  processorio,
  SAFE_CAST(data_de_entrada AS DATE) AS data_entrada,
  TRIM(UPPER(origem)) AS origem,

  -- dimensões para filtros
  UPPER(TRIM(situacao))                 AS situacao,
  TRIM(cap)                             AS cap,
  TRIM(sintese_solicitacao)             AS sintese_solicitacao,
  TRIM(mandado_de_prisao)               AS mandado_de_prisao,
  TRIM(crime_de_desobediencia)          AS crime_de_desobediencia,
  TRIM(setor_responsavel)               AS setor_responsavel,
  UPPER(TRIM(sexo))                     AS sexo,
  UPPER(TRIM(idade))                    AS idade

FROM {{ ref('int_cdi__pgm') }}
WHERE origem IS NOT NULL