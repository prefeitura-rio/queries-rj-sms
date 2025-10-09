{{ config(
    alias = "publico_alvo",
    materialized = "table"
) }}


WITH

    gestacoes_encerradas AS (
    SELECT
        cpf,
        data_diagnostico                        AS inicio,
        data_diagnostico_seguinte               AS fim,
        'Gestacao'                              AS tipo_publico
    FROM {{ ref('mart_linhas_cuidado__gestacoes') }}
    WHERE tipo_transicao = 'Encerramento Comprovado'
        AND data_diagnostico IS NOT NULL
        AND data_diagnostico_seguinte IS NOT NULL
    ),

    -- delimita a gestacao quando nao ha encerramento ou registro de parto
    gestacoes_em_andamento AS (
    SELECT
        cpf,
        data_diagnostico AS inicio,
        LEAST(DATE_ADD(data_diagnostico, INTERVAL 365 DAY), CURRENT_DATE()) AS fim, -- 12 meses, duração máxima esperada de uma gravidez considerando margem
        'Gestacao' AS tipo_publico
    FROM {{ ref('mart_linhas_cuidado__gestacoes') }}
    WHERE tipo_transicao = 'Em Andamento'
        AND data_diagnostico IS NOT NULL
    ),

    -- Puerpério: 42 dias após encerramento/parto
    puerperio AS (
    SELECT
        cpf,
        data_diagnostico_seguinte  AS inicio,
        DATE_ADD(data_diagnostico_seguinte, INTERVAL 42 DAY) AS fim,
        'Puerperio' AS tipo_publico
    FROM {{ ref('mart_linhas_cuidado__gestacoes') }}
    WHERE tipo_transicao = 'Encerramento Comprovado'
        AND data_diagnostico_seguinte IS NOT NULL
    ),

    -- ------------------------------------------------------------
    -- Criancas
    -- ------------------------------------------------------------
    criancas as (
        SELECT
            cpf,
            data_nascimento AS inicio,
            DATE_ADD(data_nascimento, INTERVAL 6 YEAR) AS fim,
            'Infancia' as tipo_publico
        FROM {{ ref("raw_prontuario_vitacare__paciente") }}
        WHERE data_nascimento > DATE_SUB(CURRENT_DATE(), INTERVAL 6 YEAR) and cpf <> 'NAO TEM'
        qualify row_number() over (partition by cpf order by source_updated_at desc) = 1
    ),

    -- ------------------------------------------------------------
    -- Junção dos casos
    -- ------------------------------------------------------------
    juncao_casos AS (
    SELECT * FROM gestacoes_encerradas
    UNION ALL SELECT * FROM gestacoes_em_andamento
    UNION ALL SELECT * FROM puerperio
    UNION ALL SELECT * FROM criancas
)

SELECT
  cpf,
  DATE(inicio) AS inicio,
  DATE(fim) AS fim,
  tipo_publico,
  STRUCT(CURRENT_TIMESTAMP() AS ultima_atualizacao) AS metadados
FROM juncao_casos
WHERE inicio <= fim