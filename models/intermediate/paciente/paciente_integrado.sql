-- This code integrates patient data from three sources:
-- rj-sms.brutos_prontuario_vitacare.paciente (VITACARE)
-- rj-sms.brutos_plataforma_vitai.paciente (VITAI)
-- rj-sms.brutos_plataforma_smsrio.paciente (SMSRIO)
-- The goal is to consolidate information such as registration data,
-- contact, address and medical record into a single view.

-- Declaration of the variable to filter by CPF (optional)
DECLARE cpf_filter STRING DEFAULT "";

-- VITACARE: Patient base table
WITH vitacare_tb AS (
    SELECT 
        cpf,
        cns,
        dados.nome,
        dados.nome_social,
        dados.data_nascimento,
        dados.genero,
        dados.raca,
        dados.obito_indicador,
        dados.obito_data,
        dados.mae_nome,
        dados.pai_nome,
        clinica_familia,
        equipe_saude_familia,
        contato,
        endereco,
        prontuario
    FROM `rj-sms.teste_paciente.vitacare`
    -- WHERE cpf = cpf_filter
),

-- VITAI: Patient base table
vitai_tb AS (
    SELECT 
        cpf,
        cns,
        dados.nome,
        dados.nome_social,
        dados.data_nascimento,
        dados.genero,
        dados.raca,
        dados.obito_indicador,
        dados.obito_data,
        dados.mae_nome,
        dados.pai_nome,
        contato,
        endereco,
        prontuario
    FROM `rj-sms.teste_paciente.vitai`
    -- WHERE cpf = cpf_filter
),

-- SMSRIO: Patient base table
smsrio_tb AS (
    SELECT 
        cpf,
        cns,
        dados.nome,
        dados.nome_social,
        dados.data_nascimento,
        dados.genero,
        dados.raca,
        dados.obito_indicador,
        dados.obito_data,
        dados.mae_nome,
        dados.pai_nome,
        contato,
        endereco,
        prontuario
    FROM `rj-sms.teste_paciente.smsrio`
    -- WHERE cpf = cpf_filter
),

---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
--  Merge data from different sources
---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--

-- CNS Dados: Merges CNS data, grouping by patient 
-- UNION 1. Vitacare | 2. Vitai | 3. SMSRIO
cns_dedup AS (
    SELECT
        cpf,
        cns,
        ROW_NUMBER() OVER (PARTITION BY cpf  ORDER BY merge_order ASC, rank ASC) AS rank,
        sistema
    FROM(
        SELECT 
            cpf,
            cns,
            rank,
            merge_order,
            ROW_NUMBER() OVER (PARTITION BY cpf, cns ORDER BY merge_order, rank ASC) AS dedup_rank,
            sistema
        FROM (
            SELECT
                cpf,
                cns.cns AS cns,
                cns.rank AS rank,
                "VITACARE" AS sistema,
                1 AS merge_order
            FROM vitacare_tb,
            UNNEST(cns) AS cns
            UNION ALL 
            SELECT 
                cpf,
                cns.cns AS cns,
                cns.rank AS rank,
                "VITAI" AS sistema,
                2 AS merge_order
            FROM vitai_tb,
            UNNEST(cns) AS cns
            UNION ALL 
            SELECT 
                cpf,
                cns.cns AS cns,
                cns.rank AS rank,
                "SMSRIO" AS sistema,
                3 AS merge_order
            FROM smsrio_tb,
            UNNEST(cns) AS cns
        )
        ORDER BY  merge_order ASC, rank ASC 
    )
    WHERE dedup_rank = 1
    ORDER BY  merge_order ASC, rank ASC 
),


cns_dados AS (
    SELECT 
        cpf,
        ARRAY_AGG(
                STRUCT(
                    cns, 
                    rank,
                    sistema
                )
        ) AS cns
    FROM cns_dedup
    GROUP BY cpf
),


-- Clinica Familia Dados: Groups family clinic data by patient.
-- ONLY VITACARE
clinica_familia_dados AS (
    SELECT
        cpf,
        clinica_familia
    FROM vitacare_tb
),

-- Equipe Saude Familia Dados: Groups family health team data by patient.
-- ONLY VITACARE
equipe_saude_familia_dados AS (
    SELECT
        cpf,
        equipe_saude_familia
    FROM vitacare_tb
),

-- Contato Dados: Merges contact data 
-- UNION: 1. Vitacare | 2. SMSRIO | 3. Vitai
contato_dados AS (
    SELECT
        COALESCE(
            vc.cpf,
            sm.cpf,
            vi.cpf
        ) AS cpf,
        STRUCT(
            ARRAY_CONCAT(vc.contato.telefone, sm.contato.telefone, vi.contato.telefone) AS telefone,
            ARRAY_CONCAT(vc.contato.email, sm.contato.email, vi.contato.email) AS email
        ) AS contato
    FROM vitacare_tb vc
    FULL OUTER JOIN smsrio_tb sm
        ON vc.cpf = sm.cpf
    FULL OUTER JOIN vitai_tb vi
        ON vc.cpf = vi.cpf
),

-- Endereco Dados: Merges address information
-- UNION: 1. Vitacare | 2. SMSRIO | 3. Vitai
endereco_dados AS (
    SELECT
        COALESCE(vc.cpf, sm.cpf, vi.cpf) AS cpf,
        ARRAY_CONCAT(
            ARRAY(SELECT AS STRUCT * FROM UNNEST(vc.endereco)),
            ARRAY(SELECT AS STRUCT * FROM UNNEST(sm.endereco)),
            ARRAY(SELECT AS STRUCT * FROM UNNEST(vi.endereco))
        ) AS endereco
    FROM vitacare_tb vc
    FULL OUTER JOIN smsrio_tb sm ON vc.cpf = sm.cpf
    FULL OUTER JOIN vitai_tb vi ON vc.cpf = vi.cpf
),


-- Prontuario Dados: Merges system medical record data
-- UNION: 1. Vitacare | 2. SMSRIO | 3. Vitai
prontuario_dados AS (
    SELECT
        COALESCE(
            vc.cpf,
            sm.cpf,
            vi.cpf
        ) AS cpf,
        STRUCT(
            vc.prontuario AS vitacare,
            sm.prontuario AS smsrio,
            vi.prontuario AS vitai
        ) AS prontuario
    FROM vitacare_tb vc
    FULL OUTER JOIN smsrio_tb sm
        ON vc.cpf = sm.cpf
    FULL OUTER JOIN vitai_tb vi
        ON vc.cpf = vi.cpf
),

-- Paciente Dados: Merges patient data
-- COALESCE
-- nome:             1. SMSRIO   | 2. Vitai    | 3. Vitacare
-- nome_social:      1. SMSRIO   | 2. Vitai    | 3. Vitacare
-- cpf:              1. SMSRIO   | 2. Vitai    | 3. Vitacare
-- data_nascimento:  1. SMSRIO   | 2. Vitacare | 3. Vitai
-- genero:           1. SMSRIO   | 2. Vitai    | 3. Vitacare
-- raca:             1. Vitacare | 2. SMSRIO   | 3. Vitai
-- obito_indicador:  1. SMSRIO   | 2. Vitai    | 3. Vitacare
-- obito_data:       1. SMSRIO   | 2. Vitai    | 3. Vitacare
-- mae_nome:         1. SMSRIO   | 2. Vitai    | 3. Vitacare
-- pai_nome:         1. SMSRIO   | 2. Vitai    | 3. Vitacare
paciente_dados AS (
    SELECT
        COALESCE(sm.cpf, vi.cpf, vc.cpf) AS cpf,
        STRUCT(
                COALESCE(sm.nome, vi.nome, vc.nome) AS nome,
                COALESCE(sm.nome_social, vi.nome_social, vc.nome_social) AS nome_social,
                COALESCE(sm.cpf, vi.cpf, vc.cpf) AS cpf,
                COALESCE(sm.data_nascimento, vi.data_nascimento, vc.data_nascimento) AS  data_nascimento,
                COALESCE(sm.genero, vi.genero, vc.genero) AS genero,
                COALESCE(vc.raca, sm.raca, vi.raca) AS raca,
                COALESCE(sm.obito_indicador, vi.obito_indicador, vc.obito_indicador) AS obito_indicador,
                COALESCE(sm.obito_data, vi.obito_data, vc.obito_data) AS obito_data,
                COALESCE(sm.mae_nome, vi.mae_nome, vc.mae_nome) AS mae_nome,
                COALESCE(sm.pai_nome, vi.pai_nome, vc.pai_nome) AS pai_nome,
                CASE 
                    WHEN sm.nome IS NOT NULL AND sm.data_nascimento IS NOT NULL AND sm.mae_nome IS NOT NULL THEN TRUE
                    ELSE FALSE 
                END AS cadastro_validado_indicador
        ) AS dados
    FROM vitacare_tb vc
    FULL OUTER JOIN smsrio_tb sm
        ON vc.cpf = sm.cpf
    FULL OUTER JOIN vitai_tb vi
        ON vc.cpf = vi.cpf
    GROUP BY sm.cpf, vi.cpf, vc.cpf, 
        sm.nome, vi.nome, vc.nome,
        sm.nome_social, vi.nome_social, vc.nome_social,
        sm.cpf, vi.cpf, vc.cpf, 
        sm.data_nascimento, vi.data_nascimento, vc.data_nascimento,
        sm.genero, vi.genero, vc.genero,
        vc.raca, sm.raca, vi.raca,
        sm.obito_indicador, vi.obito_indicador, vc.obito_indicador, 
        sm.obito_data, vi.obito_data, vc.obito_data,
        sm.mae_nome, vi.mae_nome, vc.mae_nome, 
        sm.pai_nome, vi.pai_nome, vc.pai_nome,
        CASE 
            WHEN sm.nome IS NOT NULL AND sm.data_nascimento IS NOT NULL AND sm.mae_nome IS NOT NULL THEN TRUE
            ELSE FALSE 
        END 
),

---- FINAL JOIN: Joins all the data previously processed, creating the
---- integrated table of the patients.
paciente_integrado AS (
    SELECT
        pd.cpf,
        cns.cns,
        pd.dados,
        cf.clinica_familia,
        esf.equipe_saude_familia,
        ct.contato,
        ed.endereco,
        pt.prontuario,
        STRUCT(CURRENT_TIMESTAMP() AS created_at) AS metadados
    FROM paciente_dados pd
    LEFT JOIN cns_dados cns ON pd.cpf = cns.cpf
    LEFT JOIN clinica_familia_dados cf ON pd.cpf = cf.cpf
    LEFT JOIN equipe_saude_familia_dados esf ON pd.cpf = esf.cpf
    LEFT JOIN contato_dados ct ON pd.cpf = ct.cpf
    LEFT JOIN endereco_dados ed ON pd.cpf = ed.cpf
    LEFT JOIN prontuario_dados pt ON pd.cpf = pt.cpf
)


SELECT * 
FROM paciente_integrado
WHERE EXISTS (SELECT 1 FROM UNNEST(prontuario.smsrio) WHERE id_paciente IS NOT NULL)
    AND EXISTS (SELECT 1 FROM UNNEST(prontuario.vitacare) WHERE id_paciente IS NOT NULL)
    AND EXISTS (SELECT 1 FROM UNNEST(prontuario.vitai) WHERE id_paciente IS NOT NULL)
