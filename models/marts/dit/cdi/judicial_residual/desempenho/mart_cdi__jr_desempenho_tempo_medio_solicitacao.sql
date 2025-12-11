{{ config(
  schema = "projeto_cdi",
  alias  = "jr_desempenho_tempo_medio_solicitacao",
  materialized = "table"
) }}

WITH base AS (
  SELECT
    SAFE_CAST(processo_rio AS STRING) AS processo_rio,
    SAFE_CAST(TRIM(orgao) AS STRING) AS orgao,
    SAFE_CAST(TRIM(area) AS STRING) AS area,
    REGEXP_REPLACE(TRIM(area), r'\.', '') AS codigo_ap,
    UPPER(TRIM(situacao)) AS situacao,
    LOWER(TRIM(COALESCE(solicitacao, 'não informado'))) AS solicitacao_raw,
    DATE(entrada_gat_3) AS data_entrada,
    DATE(retorno) AS data_retorno
  FROM {{ ref('int_cdi__judicial_residual') }}
  WHERE entrada_gat_3 IS NOT NULL
),

tipos AS (
  SELECT DISTINCT
    TRIM(tipo) AS tipo_solicitacao
  FROM base, UNNEST(SPLIT(solicitacao_raw, ',')) AS tipo
  WHERE TRIM(tipo) IS NOT NULL
    AND TRIM(tipo) <> ''
    AND LOWER(TRIM(tipo)) NOT IN ('não informado', 'nao informado')
),

calc AS (
  SELECT
    INITCAP(t.tipo_solicitacao) AS tipo_solicitacao,
    COALESCE(b.orgao, 'Não informado') AS orgao,
    COALESCE(b.area, 'Não informado') AS area,
    COALESCE(b.codigo_ap, 'Não informado') AS codigo_ap,
    COALESCE(b.situacao, 'Não informado') AS situacao,
    b.data_entrada,
    COUNT(DISTINCT b.processo_rio) AS total_solicitacoes,
    ROUND(AVG(DATE_DIFF(b.data_retorno, b.data_entrada, DAY)), 1) AS tempo_medio_dias
  FROM base b
  CROSS JOIN tipos t
  WHERE b.data_retorno IS NOT NULL
    AND REGEXP_CONTAINS(
      LOWER(b.solicitacao_raw),
      CONCAT(r'\b', LOWER(TRIM(t.tipo_solicitacao)), r'\b')
    )
  GROUP BY 1,2,3,4,5,6
)

SELECT
  tipo_solicitacao,
  orgao,
  area,
  codigo_ap,
  situacao,
  data_entrada,
  total_solicitacoes,
  tempo_medio_dias
FROM calc
WHERE tipo_solicitacao IS NOT NULL
ORDER BY tempo_medio_dias DESC