{{ config(
  schema = "projeto_cdi",
  alias  = "jr_desempenho_solicitacoes_prazo_mensal",
  materialized = "table"
) }}

WITH base AS (
  SELECT
    SAFE_CAST(id AS STRING) AS id,
    SAFE_CAST(TRIM(processo_rio) AS STRING) AS processo_rio,
    INITCAP(TRIM(solicitacao)) AS tipo_solicitacao,
    COALESCE(INITCAP(TRIM(orgao_para_subsidiar)), 'Não informado') AS orgao,
    COALESCE(TRIM(area), 'Não informado') AS area,
    CASE
      WHEN REGEXP_CONTAINS(TRIM(area), r'^\d+(\.\d+)?$') THEN REGEXP_REPLACE(TRIM(area), r'\.', '')
      ELSE 'Não informado'
    END AS codigo_ap,
    UPPER(TRIM(situacao)) AS situacao,
    DATE(entrada_gat_3) AS data_entrada,
    DATE(retorno) AS data_retorno,
    SAFE_CAST(prazo_dias AS INT64) AS prazo_dias,
    CASE
      WHEN prazo_dias IS NOT NULL THEN DATE_ADD(DATE(entrada_gat_3), INTERVAL prazo_dias DAY)
      ELSE NULL
    END AS data_vencimento
  FROM {{ ref('int_cdi__judicial_residual') }}
  WHERE entrada_gat_3 IS NOT NULL
),

resolvidos AS (
  SELECT *
  FROM base
  WHERE situacao = 'RESOLVIDO'
),

calc AS (
  SELECT
    *,
    CASE
      WHEN prazo_dias IS NULL OR data_retorno IS NULL THEN 'Sem Data Retorno'
      WHEN data_retorno <= data_vencimento THEN 'Dentro do Prazo'
      ELSE 'Fora do Prazo'
    END AS status_prazo,
    DATE_TRUNC(data_entrada, MONTH) AS mes_ref
  FROM resolvidos
),

dedup AS (
  SELECT * EXCEPT(rn)
  FROM (
    SELECT
      c.*,
      ROW_NUMBER() OVER (
        PARTITION BY id, mes_ref
        ORDER BY
          CASE WHEN data_retorno IS NULL THEN 1 ELSE 0 END,
          data_retorno DESC,
          data_entrada DESC
      ) AS rn
    FROM calc c
  )
  WHERE rn = 1
)

SELECT
  id,
  processo_rio,
  mes_ref,
  data_entrada,
  data_retorno,
  data_vencimento,
  prazo_dias,
  orgao,
  tipo_solicitacao,
  codigo_ap,
  area,
  situacao,
  status_prazo
FROM dedup