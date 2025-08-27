
{{
    config(
        enabled=true,
        alias="encaminhamentos",
    )
}}


WITH
-- =================================================================================================
-- PASSO 1: LER OS DADOS DO MODELO DE GESTAÇÕES
-- Ponto de partida: seleciona apenas as gestações ativas da tabela correta.
-- =================================================================================================
gestacoes_definidas AS (
    SELECT
        id_gestacao,
        id_paciente,
        cpf,
        data_inicio,
        data_fim_efetiva
    FROM {{ ref('mart_bi_gestacoes__gestacoes') }}
    -- FROM rj-sms.projeto_gestacoes.gestacoes -- Tabela corrigida
    WHERE fase_atual = 'Gestação'
),

-- =================================================================================================
-- PASSO 2: CRIAR UMA LISTA FOCADA DE IDENTIFICADORES
-- Criamos uma lista com todos os CPFs e CNS (desagrupados) APENAS das nossas gestantes.
-- =================================================================================================
paciente_identificadores AS (
    SELECT
        g.id_paciente,
        p.cpf,
        cns_paciente
    FROM gestacoes_definidas g
    JOIN {{ ref('mart_historico_clinico__paciente') }} p ON g.id_paciente = p.dados.id_paciente
    LEFT JOIN UNNEST(p.cns) AS cns_paciente
    QUALIFY ROW_NUMBER() OVER(PARTITION BY g.id_paciente, cns_paciente) = 1
),

-- =================================================================================================
-- PASSO 3: PRÉ-FILTRAR AS FONTES DE ENCAMINHAMENTO
-- Usamos a lista de identificadores para buscar em paralelo nas tabelas grandes de SISREG e SER.
-- =================================================================================================

-- CTE 3.1: sisreg_pre_filtrado
sisreg_pre_filtrado AS (
    SELECT
        pi.id_paciente,
        pi.cpf,
        s.*
    FROM {{ ref('raw_sisreg_api__solicitacoes') }} s
    JOIN paciente_identificadores pi
    ON (s.paciente_cpf = pi.cpf AND pi.cpf IS NOT NULL AND pi.cpf != '')
    OR (s.paciente_cns = pi.cns_paciente AND pi.cns_paciente IS NOT NULL AND pi.cns_paciente != '')
    WHERE s.procedimento_id IN ('0703844','0703886','0737024','0710301','0710128')
),


-- SELECT
--     *
-- FROM ser_pre_filtrado ser_filt
-- -- FROM sisreg_pre_filtrado sisreg_filt

-- CTE 3.2: ser_pre_filtrado
ser_pre_filtrado AS (
    SELECT
        pi.id_paciente,
        pi.cpf,
        ser.*
    -- FROM rj-sms-sandbox.sub_pav_us.BD_SER_PreNatal_altoRisco_2024_2025_ago_ ser
    FROM {{ ref('raw_sheets__encaminhamentos_ser') }} ser
      JOIN paciente_identificadores pi
    ON CAST(ser.cns AS STRING) = pi.cns_paciente AND pi.cns_paciente IS NOT NULL
),

-- SELECT
--     *
-- FROM ser_pre_filtrado ser_filt
-- -- FROM sisreg_pre_filtrado sisreg_filt

-- =================================================================================================
-- PASSO 4: ASSOCIAR ENCAMINHAMENTOS ÀS GESTAÇÕES CORRETAS
-- Agora, com tabelas menores, associamos os encaminhamentos pré-filtrados às suas respectivas gestações.
-- =================================================================================================

-- CTE 4.1: encaminhamento_SISREG
encaminhamento_SISREG AS (
    SELECT
        g.id_gestacao,
        DATE(s.data_solicitacao) AS data_solicitacao,
        s.solicitacao_status,
        s.solicitacao_situacao,
        s.procedimento,
        s.procedimento_id,
        s.unidade_solicitante,
        s.medico_solicitante,
        s.operador_solicitante_nome,
        s.cid_id,
        ROW_NUMBER() OVER (PARTITION BY g.id_gestacao ORDER BY s.data_solicitacao ASC) as rn_solicitacao
    FROM gestacoes_definidas g
    JOIN sisreg_pre_filtrado s ON g.id_paciente = s.id_paciente
    WHERE DATE(s.data_solicitacao) BETWEEN g.data_inicio AND COALESCE(g.data_fim_efetiva, CURRENT_DATE())
),

-- CTE 4.2: encaminhamento_SER (AJUSTADA CONFORME O SCHEMA)
encaminhamento_SER AS (
    SELECT
        g.id_gestacao,
        s.classificacao_risco,
        s.Recurso_Solicitado,
        s.Estado_Solicitacao,
        s.Codigo_cid,
        s.Descricao_cid,
        CAST(s.Dt_agendamento AS DATE) AS Dt_agendamento, -- Adicionado alias
        CAST(s.Dt_execucao AS DATE) AS Dt_execucao,       -- Adicionado alias
        s.UnidadeExecutante, -- Corrigido para corresponder ao schema
        s.Unidade_Origem,
        ROW_NUMBER() OVER (PARTITION BY g.id_gestacao ORDER BY CAST(s.Dt_Solicitacao AS DATE) ASC) as rn_solicitacao
    FROM gestacoes_definidas g
    JOIN ser_pre_filtrado s ON g.id_paciente = s.id_paciente
    WHERE CAST(s.Dt_Solicitacao AS DATE) BETWEEN g.data_inicio AND COALESCE(g.data_fim_efetiva, CURRENT_DATE())
)

-- =================================================================================================
-- PASSO 5: JUNÇÃO FINAL
-- A junção final agora é muito mais leve, pois opera sobre CTEs já processadas e filtradas.
-- =================================================================================================
SELECT
 g.id_gestacao,
 g.id_paciente,
 g.cpf,
 
 -- COLUNAS DE STATUS DE ENCAMINHAMENTO
 CASE
    WHEN sis_sol.id_gestacao IS NOT NULL OR ser_sol.id_gestacao IS NOT NULL THEN 'Sim'
    ELSE 'Não'
 END AS houve_encaminhamento,
 CASE
    WHEN sis_sol.id_gestacao IS NOT NULL AND ser_sol.id_gestacao IS NOT NULL THEN 'Ambos'
    WHEN sis_sol.id_gestacao IS NOT NULL THEN 'SISREG'
    WHEN ser_sol.id_gestacao IS NOT NULL THEN 'SER'
    ELSE NULL
 END AS origem_encaminhamento,

 -- Colunas do SISREG (primeira solicitação)
 sis_sol.data_solicitacao AS sisreg_primeira_data_solicitacao,
 sis_sol.solicitacao_status AS sisreg_primeira_status,
 sis_sol.solicitacao_situacao AS sisreg_primeira_situacao,
 sis_sol.procedimento AS sisreg_primeira_procedimento_nome,
 sis_sol.procedimento_id AS sisreg_primeira_procedimento_id,
 sis_sol.cid_id AS sisreg_primeira_cid,
 sis_sol.unidade_solicitante AS sisreg_primeira_unidade_solicitante,
 sis_sol.medico_solicitante AS sisreg_primeira_medico_solicitante,
 sis_sol.operador_solicitante_nome AS sisreg_primeira_operador_solicitante,

 -- Colunas do SER (primeira solicitação) - (AJUSTADAS CONFORME O SCHEMA)
 ser_sol.classificacao_risco AS ser_classificacao_risco,
 ser_sol.Recurso_Solicitado AS ser_recurso_solicitado,
 ser_sol.Estado_Solicitacao AS ser_estado_solicitacao,
 ser_sol.Dt_agendamento AS ser_data_agendamento, -- Corrigido: usa o campo já convertido da CTE
 ser_sol.Dt_execucao AS ser_data_execucao,     -- Corrigido: nome da coluna e usa o campo já convertido
 ser_sol.UnidadeExecutante AS ser_unidade_executante, -- Corrigido: nome da coluna
 ser_sol.Codigo_cid AS ser_cid,
 ser_sol.Descricao_cid AS ser_descricao_cid,
 ser_sol.Unidade_Origem AS ser_unidade_origem,

FROM
 gestacoes_definidas g
LEFT JOIN
 (SELECT * FROM encaminhamento_SISREG WHERE rn_solicitacao = 1) sis_sol
 ON g.id_gestacao = sis_sol.id_gestacao
LEFT JOIN
 (SELECT * FROM encaminhamento_SER WHERE rn_solicitacao = 1) ser_sol
 ON g.id_gestacao = ser_sol.id_gestacao