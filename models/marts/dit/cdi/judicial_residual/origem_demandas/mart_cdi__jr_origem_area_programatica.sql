{{ config(
  schema = "projeto_cdi",
  alias  = "jr_origem_area_programatica",
  materialized = "table"
) }}

WITH base AS (
  SELECT
    CAST(TRIM(area) AS STRING) AS area,
    DATE(entrada_gat_3) AS data_solicitacao,
    INITCAP(TRIM(orgao)) AS orgao,
    COUNT(DISTINCT processo_rio) AS total_demandas
  FROM {{ ref('int_cdi__judicial_residual') }}
  WHERE area IS NOT NULL AND TRIM(area) <> ''
  GROUP BY 1, 2, 3
),

classificada AS (
  SELECT
    area,
    data_solicitacao,
    orgao,
    total_demandas,
    CASE
      WHEN REGEXP_CONTAINS(area, r'^\s*\d+(\.\d+)?\s*$')
        THEN 'Área Programática Interna'
      ELSE 'Fora de Área Programática'
    END AS tipo_area,
    CASE
      WHEN REGEXP_CONTAINS(area, r'^\s*\d+(\.\d+)?\s*$')
        THEN REGEXP_REPLACE(TRIM(area), r'\.', '')
      ELSE NULL
    END AS codigo_ap,
    CASE
      WHEN REGEXP_CONTAINS(area, r'^\s*\d+(\.\d+)?\s*$')
        THEN CAST(REGEXP_REPLACE(TRIM(area), r'\.', '') AS INT64)
      ELSE NULL
    END AS codigo_ap_classificacao_ordem,
    CASE
      WHEN REGEXP_CONTAINS(area, r'^\s*\d+(\.\d+)?\s*$')
        THEN CONCAT('AP ', TRIM(area))
      ELSE area
    END AS area_label_exibida
  FROM base
),

coordenadas AS (
  SELECT
    CAST(TRIM(area_programatica) AS STRING) AS codigo_ap,
    AVG(CAST(endereco_latitude  AS FLOAT64))  AS latitude_ap,
    AVG(CAST(endereco_longitude AS FLOAT64))  AS longitude_ap,
    ANY_VALUE(CAST(endereco_bairro AS STRING)) AS bairro_representativo
  FROM {{ ref('dim_estabelecimento') }}
  WHERE area_programatica IS NOT NULL
    AND endereco_latitude  IS NOT NULL
    AND endereco_longitude IS NOT NULL
  GROUP BY 1
),

unificado AS (
  SELECT
    a.data_solicitacao,
    a.orgao,
    CAST(a.codigo_ap AS STRING) AS codigo_ap,
    a.codigo_ap_classificacao_ordem,
    c.latitude_ap,
    c.longitude_ap,
    c.bairro_representativo,
    a.total_demandas,
    a.tipo_area,
    a.area_label_exibida AS area_label,
    CASE
      WHEN c.latitude_ap IS NOT NULL AND c.longitude_ap IS NOT NULL
        THEN CONCAT(CAST(c.latitude_ap AS STRING), ', ', CAST(c.longitude_ap AS STRING))
      ELSE NULL
    END AS area_geo,
    CASE WHEN a.tipo_area = 'Área Programática Interna' THEN TRUE ELSE FALSE END AS eh_area_interna
  FROM classificada a
  LEFT JOIN coordenadas c
    ON a.codigo_ap = c.codigo_ap
)

SELECT
  data_solicitacao,
  orgao,
  codigo_ap,
  codigo_ap_classificacao_ordem,
  area_label AS area,
  total_demandas,
  tipo_area,
  latitude_ap,
  longitude_ap,
  area_geo,
  bairro_representativo,
  eh_area_interna,
  SUM(total_demandas) OVER () AS total_geral,
  ROUND(total_demandas / SUM(total_demandas) OVER (), 4) AS percentual_area
FROM unificado
ORDER BY tipo_area, total_demandas DESC