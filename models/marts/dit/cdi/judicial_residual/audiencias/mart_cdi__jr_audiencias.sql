{{ config(
    schema = "projeto_cdi",
    alias  = "jr_audiencias",
    materialized = "table"
) }}

WITH base AS (
  SELECT
    SAFE_CAST(processo_rio AS STRING) AS processo_rio,

    COALESCE(
      INITCAP(TRIM(REGEXP_REPLACE(orgao_para_subsidiar, r'\s+', ' '))),
      'Não informado'
    ) AS orgao_para_subsidiar,

    SAFE_CAST(DATE(entrada_gat_3) AS DATE) AS dt_entrada,
    SAFE_CAST(DATE(retorno)       AS DATE) AS dt_retorno,
    SAFE_CAST(DATE(vencimento)    AS DATE) AS dt_venc,
    SAFE_CAST(DATE(data)          AS DATE) AS dt_data,

    SAFE_CAST(prazo_dias AS INT64) AS prazo_dias,

    -- Situação original
    UPPER(TRIM(situacao)) AS situacao_raw,

    -- Situação exibida
    CASE
      WHEN TRIM(situacao) IS NULL OR TRIM(situacao) = '' THEN 'Não informado'
      ELSE INITCAP(LOWER(TRIM(situacao)))
    END AS situacao_exibicao,

    LOWER(TRIM(solicitacao)) AS solicitacao
  FROM {{ ref('int_cdi__judicial_residual') }}
  WHERE LOWER(TRIM(solicitacao)) = 'audiência'
),

-- Data de referência = dt_entrada
datas AS (
  SELECT
    *,
    dt_entrada AS data_ref
  FROM base
  WHERE dt_entrada IS NOT NULL
),

-- Classificação de status
classificada AS (
  SELECT
    *,
    CASE
      WHEN UPPER(TRIM(situacao_raw)) LIKE 'RESOLVIDO%' THEN 'Resolvido'
      WHEN UPPER(TRIM(situacao_raw)) LIKE 'PENDENTE%'  THEN 'Pendente'
      WHEN TRIM(situacao_raw) IS NULL OR TRIM(situacao_raw) = '' THEN 'Não informado'
      ELSE 'Não informado'
    END AS status_audiencia
  FROM datas
),

-- Cálculo de vencimento e prazo legível
calc AS (
  SELECT
    *,
    SAFE_CAST(DATE_DIFF(dt_venc, CURRENT_DATE(), DAY) AS INT64) AS dias_para_vencer,

    CASE
      WHEN DATE_DIFF(dt_venc, CURRENT_DATE(), DAY) < 0
        THEN CONCAT('Vencida há ', ABS(DATE_DIFF(dt_venc, CURRENT_DATE(), DAY)), ' dias')
      WHEN DATE_DIFF(dt_venc, CURRENT_DATE(), DAY) = 0
        THEN 'Vence hoje'
      ELSE CONCAT('Em ', DATE_DIFF(dt_venc, CURRENT_DATE(), DAY), ' dias')
    END AS prazo_legivel
  FROM classificada
),

-- Agregado mensal baseado na data de entrada
agregado_mensal AS (
  SELECT
    DATE_TRUNC(data_ref, MONTH)                                      AS mes_ref,
    FORMAT_DATE('%Y-%m', DATE_TRUNC(data_ref, MONTH))                AS ano_mes,
    FORMAT_DATE('%b de %Y', DATE_TRUNC(data_ref, MONTH))             AS ano_mes_label,
    EXTRACT(YEAR FROM data_ref) * 100 + EXTRACT(MONTH FROM data_ref) AS ano_mes_sort,
    status_audiencia,
    COUNT(*) AS qtd_mensal
  FROM calc
  GROUP BY 1, 2, 3, 4, 5
),

resumo_cards AS (
  SELECT
    COALESCE(SAFE_DIVIDE(COUNTIF(status_audiencia = 'Resolvido'), COUNT(*)), 0) AS pct_resolvidas,
    COALESCE(SAFE_DIVIDE(COUNTIF(status_audiencia = 'Pendente'), COUNT(*)), 0) AS pct_pendentes,
    COALESCE(SAFE_CAST(AVG(DATE_DIFF(dt_retorno, dt_entrada, DAY)) AS INT64), 0) AS tempo_medio_dias,
    MAX(data_ref) AS ultima_data_audiencia
  FROM calc
)

SELECT
  c.processo_rio,
  c.orgao_para_subsidiar,
  c.data_ref,
  c.dt_entrada,
  c.dt_retorno,
  c.dt_venc,
  c.status_audiencia,
  c.situacao_exibicao AS situacao,
  c.dias_para_vencer,
  c.prazo_legivel,
  c.situacao_raw,
  a.ano_mes,
  a.ano_mes_label,
  a.qtd_mensal,
  r.pct_resolvidas,
  r.pct_pendentes,
  r.tempo_medio_dias,
  r.ultima_data_audiencia
FROM calc c
LEFT JOIN agregado_mensal a
  ON DATE_TRUNC(c.data_ref, MONTH) = a.mes_ref
LEFT JOIN resumo_cards r
  ON TRUE
ORDER BY c.data_ref