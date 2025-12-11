{{ config(
  schema = "projeto_cdi",
  alias  = "pgm_oficios_status",
  materialized = "table"
) }}

SELECT
  SAFE_CAST(data_de_entrada AS DATE) AS data_de_entrada,
  COALESCE(TRIM(situacao), 'Não informado') AS situacao,
  COALESCE(TRIM(processorio), 'Não informado') AS processorio,
  COALESCE(TRIM(cap), 'Não informado') AS cap,
  COALESCE(TRIM(origem), 'Não informado') AS origem,
  COALESCE(TRIM(sintese_solicitacao), 'Não informado') AS sintese_solicitacao,
  COALESCE(TRIM(mandado_de_prisao), 'Não informado') AS mandado_de_prisao,
  COALESCE(TRIM(crime_de_desobediencia), 'Não informado') AS crime_de_desobediencia,
  COALESCE(TRIM(setor_responsavel), 'Não informado') AS setor_responsavel
FROM {{ ref('int_cdi__pgm') }}
WHERE data_de_entrada IS NOT NULL