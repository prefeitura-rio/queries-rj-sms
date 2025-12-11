{{ config(
    schema = "projeto_cdi",
    alias = "jr_desempenho_total_atendimentos_por_solicitacao",
    materialized = "table"
) }}

WITH base AS (
  SELECT
    SAFE_CAST(TRIM(processo_rio) AS STRING) AS processo_rio,
    COALESCE(INITCAP(TRIM(orgao)), 'Não informado') AS orgao,
    COALESCE(REGEXP_REPLACE(TRIM(area), r'\.', ''), 'Não informado') AS codigo_ap, 
    COALESCE(INITCAP(TRIM(area)), 'Não informado') AS ap,
    COALESCE(UPPER(TRIM(situacao)), 'Não informado') AS situacao,
    COALESCE(DATE(entrada_gat_3), DATE(data)) AS data_entrada,
    DATE_TRUNC(COALESCE(DATE(entrada_gat_3), DATE(data)), MONTH) AS ano_mes_dt,

    -- divide solicitações compostas (com mais de um tipo para o mesmo processo)
    SPLIT(REGEXP_REPLACE(solicitacao, r'\s*,\s*', ','), ',') AS solicitacoes
  FROM {{ ref('int_cdi__judicial_residual') }}
  WHERE TRIM(solicitacao) IS NOT NULL
),

solicitacoes_explodidas AS (
  SELECT
    processo_rio,
    COALESCE(INITCAP(TRIM(sol)), 'Não informado') AS solicitacao,
    orgao,
    codigo_ap,
    ap,
    situacao,
    data_entrada,
    ano_mes_dt
  FROM base, UNNEST(solicitacoes) AS sol
),

processos_unicos AS (
  SELECT
    solicitacao,
    processo_rio,
    ANY_VALUE(orgao) AS orgao,
    ANY_VALUE(codigo_ap) AS codigo_ap,
    ANY_VALUE(ap) AS ap,
    ANY_VALUE(situacao) AS situacao,
    ANY_VALUE(data_entrada) AS data_entrada,
    ANY_VALUE(ano_mes_dt) AS ano_mes_dt
  FROM solicitacoes_explodidas
  GROUP BY solicitacao, processo_rio
)

SELECT
  orgao,
  solicitacao AS tipo_solicitacao,
  codigo_ap AS area,
  ap,
  situacao,
  data_entrada,
  ano_mes_dt,
  COUNT(DISTINCT processo_rio) AS total_atendimentos
FROM processos_unicos
GROUP BY
  orgao, tipo_solicitacao, area, ap, situacao, data_entrada, ano_mes_dt