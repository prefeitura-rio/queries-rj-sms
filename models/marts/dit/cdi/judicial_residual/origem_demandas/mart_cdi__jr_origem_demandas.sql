{{ config(
  schema = "projeto_cdi",
  alias  = "jr_origem_demandas",
  materialized = "table"
) }}

WITH
base AS (
  SELECT
    SAFE_CAST(TRIM(processo_rio) AS STRING) AS processo_rio,
    SAFE_CAST(TRIM(orgao) AS STRING) AS orgao,
    SAFE_CAST(TRIM(area) AS STRING) AS area,
    SAFE_CAST(DATE(data) AS DATE) AS data_solicitacao
  FROM {{ ref('int_cdi__judicial_residual') }}
  WHERE processo_rio IS NOT NULL
    AND TRIM(processo_rio) <> ''
),

orgaos AS (
  SELECT
    orgao,
    TRIM(REGEXP_EXTRACT(orgao, r'[^-]+$')) AS orgao_legivel,
    CASE
      WHEN REGEXP_CONTAINS(orgao, r'Juizado Especial') THEN REGEXP_EXTRACT(orgao, r'(\d+º\s+Juizado\s+Especial)')
      WHEN REGEXP_CONTAINS(orgao, r'Plantão Judicial') THEN 'Plantão Judicial'
      WHEN REGEXP_CONTAINS(orgao, r'Vara da Infância') THEN 'Vara da Infância e Juventude'
      WHEN REGEXP_CONTAINS(orgao, r'Subsecretaria de Atenção à Saúde') THEN 'Subsecretaria de Atenção à Saúde'
      ELSE INITCAP(orgao)
    END AS orgao_norm,
    COUNT(DISTINCT processo_rio) AS total_demandas
  FROM base
  WHERE orgao IS NOT NULL
  GROUP BY 1,2,3
),

area_programatica AS (
  SELECT
    area,
    COUNT(DISTINCT processo_rio) AS total_demandas,
    CASE
      WHEN REGEXP_CONTAINS(area, r'^\s*\d+(\.\d+)?\s*$') THEN 'Área Programática Interna'
      ELSE 'Fora de Área Programática'
    END AS tipo_area,
    REGEXP_REPLACE(TRIM(area), r'\.', '') AS codigo_ap
  FROM base
  WHERE area IS NOT NULL
  GROUP BY 1
),

instancia AS (
  SELECT
    orgao,
    CASE
      WHEN REGEXP_CONTAINS(UPPER(orgao), r'TJRJ') THEN 'TJRJ'
      WHEN REGEXP_CONTAINS(UPPER(orgao), r'JFRJ') THEN 'JFRJ'
      WHEN REGEXP_CONTAINS(UPPER(orgao), r'TRT')  THEN 'TRT/MPT'
      WHEN REGEXP_CONTAINS(UPPER(orgao), r'MPT')  THEN 'TRT/MPT'
      ELSE 'Outros'
    END AS instancia
  FROM base
  GROUP BY 1,2
),

coordenadas AS (
  SELECT
    CAST(area_programatica AS STRING) AS codigo_ap,
    AVG(CAST(endereco_latitude  AS FLOAT64))  AS latitude_ap,
    AVG(CAST(endereco_longitude AS FLOAT64))  AS longitude_ap
  FROM {{ ref('dim_estabelecimento') }}
  WHERE area_programatica IS NOT NULL
  GROUP BY 1
),

resumo_cards AS (
  SELECT
    tipo_area,
    COUNT(DISTINCT b.processo_rio) AS demandas_tipo,
    COUNT(DISTINCT b.processo_rio) / SUM(COUNT(DISTINCT b.processo_rio)) OVER () AS pct_tipo
  FROM base b
  LEFT JOIN area_programatica a USING (area)
  GROUP BY 1
)

SELECT
  b.processo_rio,
  b.data_solicitacao,
  o.orgao_legivel,
  o.orgao_norm,
  o.total_demandas AS total_orgao,
  a.area,
  a.tipo_area,
  a.codigo_ap,
  a.total_demandas AS total_area,
  inst.instancia,
  c.latitude_ap,
  c.longitude_ap,
  CASE WHEN a.tipo_area = 'Área Programática Interna' THEN TRUE ELSE FALSE END AS eh_area_interna,
  r.pct_tipo AS percentual_area_card
FROM base b
LEFT JOIN orgaos o USING (orgao)
LEFT JOIN area_programatica a USING (area)
LEFT JOIN instancia inst USING (orgao)
LEFT JOIN coordenadas c USING (codigo_ap)
LEFT JOIN resumo_cards r ON r.tipo_area = a.tipo_area