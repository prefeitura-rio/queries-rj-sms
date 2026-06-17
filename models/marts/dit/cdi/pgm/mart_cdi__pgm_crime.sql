{{ config(
  schema = "projeto_cdi",
  alias  = "pgm_crime",
  materialized = "table",
  meta={"owner": "karen"}
) }}

SELECT
    processo_rio,
    data_entrada,
    UPPER(TRIM(situacao)) AS situacao,
    TRIM(cap) AS cap,
    UPPER(TRIM(origem)) AS origem,
    TRIM(sintese_solicitacao) AS sintese_solicitacao,
    UPPER(TRIM(mandado_prisao)) AS mandado_prisao,
    UPPER(TRIM(crime_desobediencia)) AS crime_desobediencia,
    TRIM(setor_responsavel) AS setor_responsavel
FROM {{ ref('int_cdi__pgm') }}
WHERE data_entrada IS NOT NULL