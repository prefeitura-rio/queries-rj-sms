{{ config(
  schema = "projeto_cdi",
  alias  = "pgm_demanda_por_origem",
  materialized = "table",
  meta={"owner": "karen"}
) }}

SELECT
  processo_rio,
  SAFE_CAST(data_entrada AS DATE) AS data_entrada,
  TRIM(UPPER(origem)) AS origem,

  UPPER(TRIM(situacao))                 AS situacao,
  TRIM(cap)                             AS cap,
  TRIM(sintese_solicitacao)             AS sintese_solicitacao,
  TRIM(mandado_prisao)                  AS mandado_prisao,
  TRIM(crime_desobediencia)             AS crime_desobediencia,
  TRIM(setor_responsavel)               AS setor_responsavel,
  UPPER(TRIM(sexo))                     AS sexo,
  UPPER(TRIM(idade))                    AS idade

FROM {{ ref('int_cdi__pgm') }}
WHERE origem IS NOT NULL