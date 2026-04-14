{{ config(
    schema = 'projeto_pic',
    alias = "publico_alvo",
    materialized = "table"
) }}

-- pega as gestantes e puerperas classificadas pelo modelo da SAP (monitor gestante)
WITH gestacoes_base AS (
    SELECT
        cpf,
        data_inicio,
        data_fim,
        data_fim_efetiva,
        fase_atual
    FROM {{ ref('mart_bi_gestacoes__gestacoes') }}
    WHERE cpf IS NOT NULL
),


-- resolve caso de gestantes com multiplas gestacoes e puerperios em andamento (pega o mais recente)
gestacoes_em_andamento AS (
    SELECT
        cpf,
        data_inicio AS inicio,
        LEAST(DATE_ADD(data_inicio, INTERVAL 300 DAY), CURRENT_DATE()) AS fim,
        'Gestacao' AS tipo_publico
    FROM gestacoes_base
    WHERE fase_atual = 'Gestação'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY cpf
        ORDER BY data_inicio DESC, data_fim DESC, data_fim_efetiva DESC
    ) = 1
),

-- Puerpério atual (até 42 dias após o parto)
puerperio_atual AS (
    SELECT
        cpf,
        data_fim_efetiva AS inicio, -- data_fim_efetiva é data de encerramento do CID quando houver; caso contrário, data_inicio + 300 dias, como encerramento automático da gestação.
        DATE_ADD(data_fim_efetiva, INTERVAL 45 DAY) AS fim,
        'Puerperio' AS tipo_publico
    FROM gestacoes_base
    WHERE fase_atual = 'Puerpério'
      AND data_fim_efetiva IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY cpf
        ORDER BY data_inicio DESC, data_fim DESC, data_fim_efetiva DESC
    ) = 1
),

criancas AS (
    SELECT
        cpf,
        data_nascimento AS inicio,
        DATE_ADD(data_nascimento, INTERVAL 6 YEAR) AS fim,
        'Infancia' AS tipo_publico
    FROM {{ ref("int_prontuario_vitacare__paciente") }}
    WHERE data_nascimento > DATE_SUB(CURRENT_DATE(), INTERVAL 6 YEAR)
      AND cpf <> 'NAO TEM'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY source_updated_at DESC) = 1
),

publico_atual AS (
    SELECT * FROM gestacoes_em_andamento
    UNION ALL
    SELECT * FROM puerperio_atual
    UNION ALL
    SELECT * FROM criancas
)

SELECT
    cpf,
    DATE(inicio) AS inicio,
    DATE(fim) AS fim,
    tipo_publico,
    STRUCT(CURRENT_TIMESTAMP() AS ultima_atualizacao) AS metadados
FROM publico_atual
WHERE inicio <= fim