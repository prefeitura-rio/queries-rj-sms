{{ config(
  schema = "projeto_cdi",
  alias  = "jr_origem_orgaos_demandas",
  materialized = "table"
) }}

WITH base AS (
  SELECT
    SAFE_CAST(id AS STRING)              AS id,              
    TRIM(orgao)                          AS orgao,
    DATE(entrada_gat_3)                  AS data_solicitacao,   
    SAFE_CAST(TRIM(area) AS STRING)      AS area,
    REGEXP_REPLACE(TRIM(area), r'\.', '') AS codigo_ap
  FROM {{ ref('int_cdi__judicial_residual') }}
  WHERE orgao IS NOT NULL
    AND TRIM(orgao) <> ''
    AND entrada_gat_3 IS NOT NULL
)

SELECT
  id,
  orgao,
  data_solicitacao,
  area,
  codigo_ap,

  CASE
    WHEN REGEXP_CONTAINS(orgao, r'Juizado Especial') THEN
      REGEXP_EXTRACT(orgao, r'(\d+º\s+Juizado\s+Especial)')
    WHEN REGEXP_CONTAINS(orgao, r'Vara da Infância') THEN
      REGEXP_EXTRACT(orgao, r'(\d+ª\s+Vara\s+da\s+Infância\s+e\s+da\s+Juventude)')
    WHEN REGEXP_CONTAINS(orgao, r'Vara de Execuções') THEN
      'Vara de Execuções de Penas'
    WHEN REGEXP_CONTAINS(orgao, r'Plantão Judicial') THEN
      'Plantão Judicial'
    WHEN REGEXP_CONTAINS(orgao, r'Delegacia') THEN
      'Delegacia de Polícia Civil'
    WHEN REGEXP_CONTAINS(orgao, r'Conselho Regional de Farmácia') THEN
      'Conselho Regional de Farmácia'
    WHEN REGEXP_CONTAINS(orgao, r'Juizado Especial Criminal') THEN
      REGEXP_EXTRACT(orgao, r'(\d+º\s+Juizado\s+Especial\s+Criminal)')
    WHEN REGEXP_CONTAINS(orgao, r'Advocacia-Geral da União') THEN
      'Advocacia-Geral da União'
    WHEN REGEXP_CONTAINS(orgao, r'Departamento de Gestão Hospitalar') THEN
      'Departamento de Gestão Hospitalar do RJ'
    WHEN REGEXP_CONTAINS(orgao, r'Subsecretaria de Atenção à Saúde') THEN
      'Subsecretaria de Atenção à Saúde'
    ELSE NULL
  END AS legenda

FROM base