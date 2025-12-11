{{ config(  
  schema = "projeto_cdi",
  alias  = "jr_desempenho_proximas_demandas",
  materialized = "table"
) }}

WITH base AS (
  SELECT
    SAFE_CAST(TRIM(id) AS STRING)                     AS id,
    SAFE_CAST(TRIM(processo_rio) AS STRING)           AS processo_rio,
    INITCAP(TRIM(solicitacao))                        AS tipo_solicitacao,
    INITCAP(TRIM(orgao_para_subsidiar))               AS orgao_para_subsidiar,
    SAFE_CAST(TRIM(orgao) AS STRING)                  AS orgao,
    SAFE_CAST(TRIM(area) AS STRING)                   AS area,
    REGEXP_REPLACE(TRIM(area), r'\.', '')             AS codigo_ap,
    DATE(entrada_gat_3)                               AS data_entrada,
    DATE(retorno)                                     AS data_retorno,
    SAFE_CAST(prazo_dias AS INT64)                    AS prazo_dias,

    -- cálculo do vencimento
    DATE_ADD(DATE(entrada_gat_3), INTERVAL SAFE_CAST(prazo_dias AS INT64) DAY)
      AS data_vencimento,

    UPPER(TRIM(situacao))                             AS situacao_raw,

    CASE
      WHEN TRIM(situacao) IS NULL OR TRIM(situacao) = '' THEN 'Não informado'
      ELSE INITCAP(LOWER(TRIM(situacao)))
    END AS situacao_exibicao

  FROM {{ ref('int_cdi__judicial_residual') }}
  WHERE entrada_gat_3 IS NOT NULL
),

calc AS (
  SELECT
    *,
    DATE_DIFF(data_vencimento, CURRENT_DATE(), DAY) AS dias_para_vencer,

    -- Prazo legível
    CASE
      WHEN data_vencimento IS NULL THEN 'Sem prazo definido'
      WHEN DATE_DIFF(data_vencimento, CURRENT_DATE(), DAY) < 0
        THEN CONCAT('Vencida há ', ABS(DATE_DIFF(data_vencimento, CURRENT_DATE(), DAY)), ' dias')
      WHEN DATE_DIFF(data_vencimento, CURRENT_DATE(), DAY) = 0
        THEN 'Vence hoje'
      ELSE CONCAT('Em ', DATE_DIFF(data_vencimento, CURRENT_DATE(), DAY), ' dias')
    END AS prazo_legivel,

    -- Status do vencimento
    CASE
      WHEN data_vencimento IS NULL THEN 'Sem prazo definido'
      WHEN DATE_DIFF(data_vencimento, CURRENT_DATE(), DAY) < 0
        THEN 'Vencida'
      WHEN DATE_DIFF(data_vencimento, CURRENT_DATE(), DAY) BETWEEN 0 AND 7
        THEN 'A vencer (≤7 dias)'
      WHEN DATE_DIFF(data_vencimento, CURRENT_DATE(), DAY) BETWEEN 8 AND 15
        THEN 'A vencer (8–15 dias)'
      ELSE 'Dentro do Prazo (>15 dias)'
    END AS status_vencimento

  FROM base
),

dedup AS (
  SELECT * EXCEPT(rn)
  FROM (
    SELECT
      c.*,
      ROW_NUMBER() OVER (
        PARTITION BY id
        ORDER BY
          CASE WHEN data_retorno IS NULL THEN 1 ELSE 0 END,
          data_vencimento DESC,
          data_entrada DESC
      ) AS rn
    FROM calc c
  )
  WHERE rn = 1
)

SELECT
  COALESCE(id, 'Não informado')                    AS id,
  COALESCE(processo_rio, 'Não informado')          AS processo_rio,
  COALESCE(tipo_solicitacao, 'Não informado')      AS tipo_solicitacao,
  COALESCE(orgao_para_subsidiar, 'Não informado')  AS orgao_para_subsidiar,
  COALESCE(orgao, 'Não informado')                 AS orgao,
  COALESCE(area, 'Não informado')                  AS area,
  COALESCE(codigo_ap, 'Não informado')             AS codigo_ap,
  COALESCE(situacao_exibicao, 'Não informado')     AS situacao,
  COALESCE(situacao_raw, 'Não informado')          AS situacao_raw,
  data_entrada,
  data_vencimento,
  prazo_dias,
  dias_para_vencer,
  prazo_legivel,
  COALESCE(status_vencimento, 'Não informado')     AS status_vencimento

FROM dedup
WHERE UPPER(TRIM(situacao_raw)) LIKE 'PENDENTE%'
ORDER BY data_vencimento ASC