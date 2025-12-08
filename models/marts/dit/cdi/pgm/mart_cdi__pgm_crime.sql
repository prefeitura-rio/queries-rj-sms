{{ config(
  schema = "projeto_cdi",
  alias  = "pgm_crime",
  materialized = "table"
) }}

SELECT
    processorio,
    data_de_entrada,
    UPPER(TRIM(situacao)) AS situacao,
    TRIM(cap) AS cap,
    UPPER(TRIM(origem)) AS origem,
    TRIM(sintese_solicitacao) AS sintese_solicitacao,
    UPPER(TRIM(mandado_de_prisao)) AS mandado_de_prisao,
    UPPER(TRIM(crime_de_desobediencia)) AS crime_de_desobediencia,
    TRIM(setor_responsavel) AS setor_responsavel,
FROM {{ ref('int_cdi__pgm') }}
WHERE data_de_entrada IS NOT NULL