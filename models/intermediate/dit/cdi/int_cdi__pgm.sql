{{ config(
  schema = "intermediario_cdi",
  alias  = "pgm",
  materialized = "table"
) }}

WITH src AS (
  SELECT * 
  FROM {{ ref('raw_cdi__pgm') }}
  WHERE NOT (
    data_de_entrada IS NULL
    AND processo_judicial IS NULL
    AND processorio IS NULL
  )
),

calc AS (
  SELECT
    processorio,
    procuradora,
    requerente,
    processo_judicial,
    origem,
    data_de_entrada,
    data_de_saida,
    data_de_saida_para_pgm AS data_saida_pgm,
    prazo,
    mes_ano,
    sexo,
    idade,
    hospital_de_origem,
    cap,
    erro_medico,
    acp,
    multa_bloqueio_de_verba_indenizacao AS tipo_indenizacao,
    valor,
    mandado_de_prisao,
    crime_de_desobediencia,
    patologia_assunto,
    solicitacao,
    sintese_de_solicitacao AS sintese_solicitacao,
    setor_responsavel_pela_resposta AS setor_responsavel,
    prazo_dias,
    situacao,
    pendencias,
    observacoes,

    CASE
      WHEN UPPER(situacao) LIKE '%RESOLVID%' THEN 'Resolvido'
      WHEN prazo IS NULL THEN 'Sem Prazo'
      WHEN CURRENT_DATE() > prazo THEN 'Vencido'
      WHEN DATE_DIFF(prazo, CURRENT_DATE(), DAY) <= 3 THEN 'A Vencer (≤3 dias)'
      ELSE 'Dentro do Prazo'
    END AS status_prazo,

    DATE_DIFF(prazo, CURRENT_DATE(), DAY) AS dias_para_vencer,
    FORMAT_DATE('%Y-%m', data_de_entrada) AS mes_referencia

  FROM src
)

SELECT * FROM calc