{{ config(
    schema = 'projeto_pic',
    alias = "publico_alvo",
    materialized = "table"
) }}

WITH
-- Gestantes em andamento (fase atual)
gestacoes_em_andamento AS (
    SELECT
        cpf,
        data_diagnostico AS inicio,
        LEAST(DATE_ADD(data_diagnostico, INTERVAL 300 DAY), CURRENT_DATE()) AS fim,
        'Gestacao' AS tipo_publico
    FROM {{ ref('mart_linhas_cuidado__gestacoes') }}
    WHERE tipo_transicao = 'Em Andamento'
      AND data_diagnostico IS NOT NULL
),

-- Puerpério atual (até 42 dias após o parto)
puerperio_atual AS (
    SELECT
        cpf,
        data_diagnostico_seguinte AS inicio,
        DATE_ADD(data_diagnostico_seguinte, INTERVAL 42 DAY) AS fim,
        'Puerperio' AS tipo_publico
    FROM {{ ref('mart_linhas_cuidado__gestacoes') }}
    WHERE tipo_transicao = 'Encerramento Comprovado'
      AND data_diagnostico_seguinte IS NOT NULL
      AND CURRENT_DATE() BETWEEN data_diagnostico_seguinte AND DATE_ADD(data_diagnostico_seguinte, INTERVAL 42 DAY)
),

-- Crianças até 6 anos (fase atual)
criancas AS (
    SELECT
        cpf,
        data_nascimento AS inicio,
        DATE_ADD(data_nascimento, INTERVAL 6 YEAR) AS fim,
        'Infancia' AS tipo_publico
    FROM {{ ref("raw_prontuario_vitacare__paciente") }}
    WHERE data_nascimento > DATE_SUB(CURRENT_DATE(), INTERVAL 6 YEAR)
      AND cpf <> 'NAO TEM'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY source_updated_at DESC) = 1
),

publico_atual AS (
    SELECT * FROM gestacoes_em_andamento
    UNION ALL SELECT * FROM puerperio_atual
    UNION ALL SELECT * FROM criancas
)

SELECT
  cpf,
  DATE(inicio) AS inicio,
  DATE(fim) AS fim,
  tipo_publico,
  STRUCT(CURRENT_TIMESTAMP() AS ultima_atualizacao) AS metadados
FROM publico_atual
WHERE inicio <= fim