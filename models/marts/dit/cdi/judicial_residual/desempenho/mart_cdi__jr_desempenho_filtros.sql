{{ config(
  schema = "projeto_cdi",
  alias  = "jr_desempenho_filtros",
  materialized = "view"
) }}

SELECT DISTINCT
  COALESCE(SAFE_CAST(TRIM(orgao) AS STRING), 'Não informado') AS orgao,
  COALESCE(INITCAP(TRIM(solicitacao)), 'Não informado') AS tipo_solicitacao,
  COALESCE(REGEXP_REPLACE(TRIM(area), r'\.', ''), 'Não informado') AS codigo_ap,
  COALESCE(SAFE_CAST(TRIM(area) AS STRING), 'Não informado') AS area,
  COALESCE(UPPER(TRIM(situacao)), 'Não informado') AS situacao,
  COALESCE(DATE_TRUNC(DATE(entrada_gat_3), MONTH), DATE '1900-01-01') AS ano_mes_dt
FROM {{ ref('int_cdi__judicial_residual') }}
WHERE entrada_gat_3 IS NOT NULL