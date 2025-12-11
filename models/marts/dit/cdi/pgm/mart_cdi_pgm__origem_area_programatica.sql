{{ config(
  schema = "mart_cdi",
  alias  = "pgm_origem_area_programatica",
  materialized = "table"
) }}

WITH base AS (
  SELECT
    processorio,
    SAFE_CAST(data_de_entrada AS DATE) AS data_entrada,
    TRIM(cap) AS cap,
    UPPER(TRIM(sintese_solicitacao)) AS sintese_solicitacao,

    -- dimensões para filtros
    UPPER(TRIM(situacao))                 AS situacao,
    TRIM(origem)                          AS origem,
    TRIM(setor_responsavel)               AS setor_responsavel,
    TRIM(mandado_de_prisao)               AS mandado_de_prisao,
    TRIM(crime_de_desobediencia)          AS crime_de_desobediencia

  FROM {{ ref('int_cdi__pgm') }}
  WHERE cap IS NOT NULL
),

-- Agregação por CAP
agregado AS (
  SELECT
    cap,
    data_entrada,
    sintese_solicitacao,
    
    ANY_VALUE(situacao)            AS situacao,
    ANY_VALUE(origem)              AS origem,
    ANY_VALUE(setor_responsavel)   AS setor_responsavel,
    ANY_VALUE(mandado_de_prisao)   AS mandado_de_prisao,
    ANY_VALUE(crime_de_desobediencia) AS crime_de_desobediencia,
    COUNT(DISTINCT processorio) AS total_demandas

  FROM base
  GROUP BY cap, data_entrada, sintese_solicitacao
),

-- Classificação de área programática
classificada AS (
  SELECT
    *,
    CASE
      WHEN REGEXP_CONTAINS(cap, r'^\s*\d+(\.\d+)?\s*$') THEN 'Área Programática Interna'
      ELSE 'Fora do Município'
    END AS tipo_area,

    CASE
      WHEN REGEXP_CONTAINS(cap, r'^\s*\d+(\.\d+)?\s*$')
        THEN REGEXP_REPLACE(cap, r'\.', '')
      ELSE NULL
    END AS codigo_ap,

    CASE
      WHEN REGEXP_CONTAINS(cap, r'^\s*\d+(\.\d+)?\s*$')
        THEN CAST(REGEXP_REPLACE(cap, r'\.', '') AS INT64)
      ELSE NULL
    END AS codigo_ap_classificacao_ordem,

    CASE
      WHEN REGEXP_CONTAINS(cap, r'^\s*\d+(\.\d+)?\s*$')
        THEN CONCAT('AP ', cap)
      ELSE 'Fora do Município'
    END AS cap_label_exibida
  FROM agregado
),

-- Coordenadas por AP
coordenadas AS (
  SELECT
    CAST(area_programatica AS STRING) AS codigo_ap,
    AVG(CAST(endereco_latitude  AS FLOAT64)) AS latitude_ap,
    AVG(CAST(endereco_longitude AS FLOAT64)) AS longitude_ap,
    ANY_VALUE(endereco_bairro) AS bairro_representativo
  FROM {{ ref('dim_estabelecimento') }}
  WHERE area_programatica IS NOT NULL
  GROUP BY area_programatica
),

-- União das áreas com coordenadas
unificado AS (
  SELECT
    a.*,
    c.latitude_ap,
    c.longitude_ap,
    c.bairro_representativo,
    CASE
      WHEN c.latitude_ap IS NOT NULL THEN CONCAT(c.latitude_ap, ', ', c.longitude_ap)
    END AS area_geo,
    CASE WHEN tipo_area = 'Área Programática Interna' THEN TRUE ELSE FALSE END AS eh_area_interna
  FROM classificada a
  LEFT JOIN coordenadas c ON a.codigo_ap = c.codigo_ap
)

SELECT
  data_entrada,
  COALESCE(cap, 'Não informado') AS cap,
  sintese_solicitacao,
  situacao,
  origem,
  setor_responsavel,
  mandado_de_prisao,
  crime_de_desobediencia,
  codigo_ap,
  codigo_ap_classificacao_ordem,
  cap_label_exibida AS area,
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