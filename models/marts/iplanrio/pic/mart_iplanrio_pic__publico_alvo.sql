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
        data_inicio AS inicio,
        LEAST(DATE_ADD(data_inicio, INTERVAL 300 DAY), CURRENT_DATE()) AS fim,
        'Gestacao' AS tipo_publico
    FROM {{ ref('mart_bi_gestacoes__gestacoes') }}
    WHERE fase_atual = 'Gestação'
),

-- Puerpério atual (até 42 dias após o parto)
puerperio_atual AS (
    SELECT
        cpf,
        data_fim AS inicio,
        DATE_ADD(data_fim, INTERVAL 45 DAY) AS fim,
        'Puerperio' AS tipo_publico
    FROM {{ ref('mart_bi_gestacoes__gestacoes') }}
    WHERE fase_atual = 'Puerpério'
),

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