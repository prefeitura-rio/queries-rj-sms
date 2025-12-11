{{ config(
  schema = "intermediario_cdi",
  alias  = "judicial_residual",
  materialized = "table"
) }}

WITH src AS (
  SELECT * FROM {{ ref('raw_cdi__judicial_residual') }}
  
  -- Remove linhas nao preenchidas
  WHERE NOT (
    entrada_gat_3 IS NULL
    AND processo_rio IS NULL
    AND processo IS NULL
    AND oficio IS NULL
  )
),

calc AS (
  SELECT
    -- ID CANÔNICO para casos sem numero de processo
    CASE
        WHEN processo_rio IS NOT NULL
            AND UPPER(processo_rio) != 'E-MAIL'
          THEN processo_rio

        WHEN no_oficio IS NOT NULL
          THEN no_oficio

        WHEN oficio IS NOT NULL
          THEN oficio
        
        WHEN processo IS NOT NULL
          THEN processo

        -- Caso não exista nenhum identificador
        ELSE CONCAT(
              'SEM_ID_',
              CAST(entrada_gat_3 AS STRING), '_',
              REGEXP_REPLACE(TRIM(COALESCE(solicitacao, 'sem_solicitacao')), r'\s+', '_'), '_',
              REGEXP_REPLACE(TRIM(COALESCE(orgao_para_subsidiar, 'sem_orgao')), r'\s+', '_')
        )
    END AS id,
    processo_rio,
    mrj_e_parte,
    oficio,
    orgao,
    processo,
    assunto,

    CASE
      WHEN LOWER(TRIM(solicitacao)) IN ('exame', 'exames') THEN 'exame'
      ELSE LOWER(TRIM(solicitacao))
    END AS solicitacao,

    area,
    sexo,
    idade,
    prazo_dias,
    orgao_para_subsidiar,
    no_oficio,
    observacoes,

    CASE
      WHEN UPPER(TRIM(situacao)) = '#REF!' THEN NULL
      ELSE UPPER(TRIM(situacao))
    END AS situacao,

    -- Datas
    data,
    entrada_gat_3,
    vencimento,
    retorno,
    data_de_saida,
    data_do_oficio,
    pg_pas_dta_sfc,

    EXTRACT(YEAR FROM entrada_gat_3)       AS ano_ref,
    EXTRACT(MONTH FROM entrada_gat_3)      AS mes_ref,
    EXTRACT(QUARTER FROM entrada_gat_3)    AS trimestre_ref,

    DATE_TRUNC(entrada_gat_3,  MONTH)      AS ano_mes_dt,
    DATE_TRUNC(vencimento,     MONTH)      AS ano_mes_venc_dt,
    DATE_TRUNC(retorno,        MONTH)      AS ano_mes_retorno_dt,

    -- Prazo restante (dias)
    CASE WHEN vencimento IS NULL THEN NULL
         ELSE DATE_DIFF(vencimento, CURRENT_DATE(), DAY)
    END AS dias_para_vencer,

    -- Status de prazo
    CASE
      WHEN vencimento IS NULL THEN 'sem_vencimento'
      WHEN CURRENT_DATE() > vencimento AND (retorno IS NULL OR retorno > vencimento)
        THEN 'atrasado'
      WHEN retorno IS NOT NULL AND retorno <= vencimento
        THEN 'concluido_no_prazo'
      WHEN retorno IS NOT NULL AND retorno > vencimento
        THEN 'concluido_fora_do_prazo'
      ELSE 'em_andamento'
    END AS status_prazo

  FROM src
)

SELECT * FROM calc