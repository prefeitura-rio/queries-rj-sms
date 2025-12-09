{{ config( 
  schema = "projeto_cdi",
  alias  = "jr_desempenho_cards",
  materialized = "table"
) }}

WITH base AS (
  SELECT
    SAFE_CAST(id AS STRING) AS id,
    SAFE_CAST(TRIM(orgao) AS STRING) AS orgao,
    COALESCE(SAFE_CAST(TRIM(area) AS STRING), 'Não informado') AS area,

    CASE
      WHEN REGEXP_CONTAINS(TRIM(area), r'^\d+(\.\d+)?$')
        THEN REGEXP_REPLACE(TRIM(area), r'\.', '')
      ELSE 'Não informado'
    END AS codigo_ap,

    UPPER(TRIM(situacao)) AS situacao,

    DATE(entrada_gat_3) AS data_entrada,
    DATE_TRUNC(DATE(entrada_gat_3), MONTH) AS ano_mes_dt,
    DATE(retorno) AS data_retorno,
    SAFE_CAST(prazo_dias AS INT64) AS prazo_dias,

    DATE_ADD(DATE(entrada_gat_3), INTERVAL SAFE_CAST(prazo_dias AS INT64) DAY)
      AS data_vencimento,

    SPLIT(REGEXP_REPLACE(COALESCE(solicitacao, ''), r'\s*,\s*', ','), ',') AS solicitacoes_arr

  FROM {{ ref('int_cdi__judicial_residual') }}
  WHERE entrada_gat_3 IS NOT NULL
),

-- Normaliza tipo de solicitação
base_tipificada AS (
  SELECT
    *,
    CASE
      WHEN solicitacoes_arr IS NULL OR ARRAY_LENGTH(solicitacoes_arr) = 0
        THEN 'Não informado'
      WHEN ARRAY_LENGTH(solicitacoes_arr) = 1
        THEN INITCAP(TRIM(solicitacoes_arr[OFFSET(0)]))
      ELSE 'Múltiplas'
    END AS tipo_solicitacao
  FROM base
),

calc AS (
  SELECT
    *,
    CASE
      WHEN data_retorno IS NULL OR data_entrada IS NULL THEN NULL
      ELSE DATE_DIFF(data_retorno, data_entrada, DAY)
    END AS dias_atendimento,

    CASE
      WHEN data_retorno IS NULL THEN 'Pendente'
      WHEN data_vencimento IS NULL THEN 'Sem prazo'
      WHEN data_retorno <= data_vencimento THEN 'Dentro do Prazo'
      ELSE 'Fora do Prazo'
    END AS status_prazo
  FROM base_tipificada
)

SELECT
  id,
  data_entrada,
  data_retorno,
  data_vencimento,
  prazo_dias,
  ano_mes_dt,
  orgao,
  tipo_solicitacao,
  codigo_ap,
  area,
  situacao,
  status_prazo,
  dias_atendimento

FROM calc