{{ config(
    materialized = "table",
    schema = "projeto_subgeral_indicadores",
    alias = "indicadores_aps"
) }}

-- Seleciona tipos de atendimento com pelo menos 30% realizados por médicos (CBO 225 ou 2231)
WITH tipos_com_medico_30pct AS (
  SELECT
    tipo
  FROM {{ ref("raw_prontuario_vitacare__atendimento") }}
  WHERE tipo IS NOT NULL
  GROUP BY tipo
  HAVING 
    SUM(
      CASE 
        WHEN LEFT(cbo_profissional, 3) = '225'
          OR LEFT(cbo_profissional, 4) = '2231'
        THEN 1 ELSE 0 
      END
    ) * 1.0 / COUNT(*) >= 0.30
    AND tipo NOT LIKE '%Gestão%'
    AND tipo <> 'Procedimentos Radiológicos'
    AND tipo <> 'Recomendações Clínicas'
),

-- Filtra atendimentos válidos com médico nos últimos 12 meses
atendimentos_validos AS (
  SELECT DISTINCT
    cpf,
    SAFE_CAST(datahora_inicio AS DATE) AS data_atendimento
  FROM {{ ref("raw_prontuario_vitacare__atendimento") }}
  WHERE
    tipo IN (SELECT tipo FROM tipos_com_medico_30pct)
    AND (
      LEFT(cbo_profissional, 3) = '225'
      OR LEFT(cbo_profissional, 4) = '2231'
    )
    AND SAFE_CAST(datahora_inicio AS DATE) IS NOT NULL
    AND cpf IS NOT NULL
    AND SAFE_CAST(datahora_inicio AS DATE)
      BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
          AND CURRENT_DATE()
),

-- Conta consultas por paciente
pacientes_contagem AS (
  SELECT
    cpf,
    COUNT(*) AS total_consultas
  FROM atendimentos_validos
  GROUP BY cpf
)

-- Indicadores de acesso à Atenção Primária à Saúde
SELECT
  CURRENT_DATETIME() AS data_referencia,
  -- Total de pacientes com qualquer cadastro (definitivo e temporário)
  (SELECT COUNT(*) FROM {{ ref("raw_prontuario_vitacare__paciente") }}) AS acesso_potencial,
  -- Pacientes com pelo menos 1 consulta médica nos últimos 12 meses
  (SELECT COUNT(*) FROM pacientes_contagem WHERE total_consultas >= 1) AS acesso_realizado,
  -- Pacientes com 3 ou mais consultas médicas no mesmo período
  (SELECT COUNT(*) FROM pacientes_contagem WHERE total_consultas >= 3) AS acesso_efetivo