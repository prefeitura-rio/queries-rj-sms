{{ config(
  schema = "projeto_cdi",
  alias  = "jr_origem_audiencias_instancia",
  materialized = "table"
) }}

WITH classificada AS (
  SELECT DISTINCT
    SAFE_CAST(TRIM(processo_rio) AS STRING)                AS processo_rio,
    DATE(entrada_gat_3)                                     AS data_solicitacao,
    CASE
      WHEN REGEXP_CONTAINS(UPPER(orgao), r'TJRJ') THEN 'TJRJ'
      WHEN REGEXP_CONTAINS(UPPER(orgao), r'JFRJ') THEN 'JFRJ'
      WHEN REGEXP_CONTAINS(UPPER(orgao), r'TRT')  THEN 'TRT/MPT'
      WHEN REGEXP_CONTAINS(UPPER(orgao), r'MPT')  THEN 'TRT/MPT'
      ELSE 'Outros'
    END                                                     AS instancia,
    SAFE_CAST(TRIM(orgao) AS STRING)                        AS orgao,
    SAFE_CAST(TRIM(area)  AS STRING)                        AS area,
    REGEXP_REPLACE(TRIM(area), r'\.', '')                   AS codigo_ap
  FROM {{ ref('int_cdi__judicial_residual') }}
  WHERE orgao IS NOT NULL
    AND TRIM(orgao) <> ''
    AND LOWER(TRIM(solicitacao)) LIKE '%audiência%'
)

SELECT
  processo_rio,
  data_solicitacao,
  instancia,
  orgao,
  area,
  codigo_ap
FROM classificada