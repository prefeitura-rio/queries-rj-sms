{{
    config(
        alias="paciente",
        materialized="table",
        schema="saude_dados_mestres"
    )
}}

-- This code integrates patient data from three sources:
-- rj-sms.brutos_prontuario_vitacare.paciente (VITACARE)
-- rj-sms.brutos_plataforma_vitai.paciente (VITAI)
-- rj-sms.brutos_plataforma_smsrio.paciente (SMSRIO)
-- The goal is to consolidate information such as registration data,
-- contact, address and medical record into a single view.

-- Declaration of the variable to filter by CPF (optional)
-- DECLARE cpf_filter STRING DEFAULT "";

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
    FROM {{ ref('int_historico_clinico__paciente__vitacare') }},
    UNNEST(dados) AS dados
    WHERE dados.rank=1
    -- AND cpf = cpf_filter
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
    FROM {{ ref('int_historico_clinico__paciente__vitai') }},
    UNNEST(dados) AS dados
    WHERE dados.rank=1
    -- AND cpf = cpf_filter
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
    FROM {{ ref("int_historico_clinico__paciente__smsrio") }},
    UNNEST(dados) AS dados
    WHERE dados.rank=1
    -- AND cpf = cpf_filter
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
                    cns
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
telefone_dedup AS (
    SELECT
        cpf,
        valor, 
        ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY merge_order ASC, rank ASC) AS rank,
        sistema
    FROM (
        SELECT 
            cpf,
            valor,
            rank,
            merge_order,
            ROW_NUMBER() OVER (PARTITION BY cpf, valor ORDER BY merge_order, rank ASC) AS dedup_rank,
            sistema
        FROM (
            SELECT
                cpf,
                telefone.valor, 
                telefone.rank,
                "VITACARE" AS sistema,
                1 AS merge_order
            FROM vitacare_tb,
            UNNEST(contato.telefone) AS telefone -- Expandindo os elementos da array struct de telefone
            UNION ALL 
            SELECT 
                cpf,
                telefone.valor, 
                telefone.rank,
                "SMSRIO" AS sistema,
                2 AS merge_order
            FROM smsrio_tb,
            UNNEST(contato.telefone) AS telefone
            UNION ALL 
            SELECT 
                cpf,
                telefone.valor, 
                telefone.rank,
                "VITAI" AS sistema,
                3 AS merge_order
            FROM vitai_tb,
            UNNEST(contato.telefone) AS telefone
        )
        ORDER BY merge_order ASC, rank ASC
    )
    WHERE dedup_rank = 1
    ORDER BY merge_order ASC, rank ASC
),

email_dedup AS (
    SELECT
        cpf,
        valor, 
        ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY merge_order ASC, rank ASC) AS rank,
        sistema
    FROM (
        SELECT 
            cpf,
            valor,
            rank,
            merge_order,
            ROW_NUMBER() OVER (PARTITION BY cpf, valor ORDER BY merge_order, rank ASC) AS dedup_rank,
            sistema
        FROM (
            SELECT
                cpf,
                email.valor, 
                email.rank,
                "VITACARE" AS sistema,
                1 AS merge_order
            FROM vitacare_tb,
            UNNEST(contato.email) AS email -- Expandindo os elementos da array struct de email
            UNION ALL 
            SELECT 
                cpf,
                email.valor, 
                email.rank,
                "SMSRIO" AS sistema,
                2 AS merge_order
            FROM smsrio_tb,
            UNNEST(contato.email) AS email
            UNION ALL 
            SELECT 
                cpf,
                email.valor, 
                email.rank,
                "VITAI" AS sistema,
                3 AS merge_order
            FROM vitai_tb,
            UNNEST(contato.email) AS email
        )
        ORDER BY merge_order ASC, rank ASC
    )
    WHERE dedup_rank = 1
    ORDER BY merge_order ASC, rank ASC
),

contato_dados AS (
    SELECT 
        COALESCE(t.cpf, e.cpf) AS cpf,
        STRUCT(
            ARRAY_AGG(STRUCT(t.valor, t.sistema,t.rank)) AS telefone,
            ARRAY_AGG(STRUCT(e.valor, e.sistema, e.rank)) AS email    
        ) AS contato
    FROM telefone_dedup t
    FULL OUTER JOIN email_dedup e
        ON t.cpf = e.cpf
    GROUP BY COALESCE(t.cpf, e.cpf)
),

-- Endereco Dados: Merges address information
-- UNION: 1. Vitacare | 2. SMSRIO | 3. Vitai
endereco_dedup AS (
    SELECT
        cpf,
        cep, 
        tipo_logradouro, 
        logradouro, 
        numero, 
        complemento, 
        bairro, 
        cidade, 
        estado, 
        datahora_ultima_atualizacao,
        ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY merge_order ASC, rank ASC) AS rank,
        sistema
    FROM (
        SELECT 
            cpf,
            cep,
            tipo_logradouro,
            logradouro,
            numero,
            complemento,
            bairro,
            cidade,
            estado,
            datahora_ultima_atualizacao,
            merge_order,
            rank,
            ROW_NUMBER() OVER (PARTITION BY cpf, datahora_ultima_atualizacao ORDER BY merge_order, rank ASC) AS dedup_rank,
            sistema
        FROM (
            SELECT
                cpf,
                endereco.cep, 
                endereco.tipo_logradouro, 
                endereco.logradouro, 
                endereco.numero, 
                endereco.complemento, 
                endereco.bairro, 
                endereco.cidade, 
                endereco.estado, 
                endereco.datahora_ultima_atualizacao,
                endereco.rank,
                "VITACARE" AS sistema,
                1 AS merge_order
            FROM vitacare_tb,
            UNNEST(endereco) AS endereco -- Expandindo os elementos da array struct de endereço
            UNION ALL 
            SELECT 
                cpf,
                endereco.cep, 
                endereco.tipo_logradouro, 
                endereco.logradouro, 
                endereco.numero, 
                endereco.complemento, 
                endereco.bairro, 
                endereco.cidade, 
                endereco.estado, 
                endereco.datahora_ultima_atualizacao,
                endereco.rank,
                "SMSRIO" AS sistema,
                2 AS merge_order
            FROM smsrio_tb,
            UNNEST(endereco) AS endereco
            UNION ALL 
            SELECT 
                cpf,
                endereco.cep, 
                endereco.tipo_logradouro, 
                endereco.logradouro, 
                endereco.numero, 
                endereco.complemento, 
                endereco.bairro, 
                endereco.cidade, 
                endereco.estado, 
                endereco.datahora_ultima_atualizacao,
                endereco.rank,
                "VITAI" AS sistema,
                3 AS merge_order
            FROM vitai_tb,
            UNNEST(endereco) AS endereco
        )
        ORDER BY merge_order ASC, rank ASC
    )
    WHERE dedup_rank = 1
    ORDER BY merge_order ASC, rank ASC
),

endereco_dados AS (
    SELECT 
        cpf,
        ARRAY_AGG(STRUCT(
            cep, 
            tipo_logradouro, 
            logradouro, 
            numero, 
            complemento, 
            bairro, 
            cidade, 
            estado, 
            datahora_ultima_atualizacao,
            sistema,
            rank
        )) AS endereco
    FROM endereco_dedup
    GROUP BY cpf
),


-- Prontuario Dados: Merges system medical record data
-- UNION: 1. Vitacare | 2. SMSRIO | 3. Vitai
prontuario_dedup AS (
    SELECT
        cpf,
        sistema, 
        id_cnes, 
        id_paciente, 
        ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY merge_order ASC, rank ASC) AS rank
    FROM (
        SELECT 
            cpf,
            sistema, 
            id_cnes, 
            id_paciente, 
            rank,
            merge_order,
            ROW_NUMBER() OVER (PARTITION BY cpf, id_cnes, id_paciente ORDER BY merge_order, rank ASC) AS dedup_rank
        FROM (
            SELECT
                vc.cpf,
                "VITACARE" AS sistema,
                prontuario.id_cnes, 
                prontuario.id_paciente, 
                prontuario.rank,
                1 AS merge_order
            FROM vitacare_tb vc,
            UNNEST(prontuario) AS prontuario
            UNION ALL 
            SELECT 
                sm.cpf,
                "SMSRIO" AS sistema,
                prontuario.id_cnes, 
                prontuario.id_paciente, 
                prontuario.rank,
                2 AS merge_order
            FROM smsrio_tb sm,
            UNNEST(prontuario) AS prontuario
            UNION ALL 
            SELECT 
                vi.cpf,
                "VITAI" AS sistema,
                prontuario.id_cnes, 
                prontuario.id_paciente, 
                prontuario.rank,
                3 AS merge_order
            FROM vitai_tb vi,
            UNNEST(prontuario) AS prontuario
        )
        ORDER BY merge_order ASC, rank ASC
    )
    WHERE dedup_rank = 1
    ORDER BY merge_order ASC, rank ASC
),

prontuario_dados AS (
    SELECT 
        cpf,
        ARRAY_AGG(STRUCT(
            sistema, 
            id_cnes, 
            id_paciente, 
            rank
        )) AS prontuario
    FROM prontuario_dedup
    GROUP BY cpf
),


-- Paciente Dados: Merges patient data
all_cpfs AS (
    SELECT 
        DISTINCT cpf
    FROM (
        SELECT 
            cpf
        FROM vitacare_tb
        UNION ALL
        SELECT 
            cpf
        FROM vitai_tb
        UNION ALL
        SELECT 
            cpf
        FROM smsrio_tb
    )
),

-- merge priority:
-- nome:             1. SMSRIO   | 2. Vitacare  | 3. Vitai
-- nome_social:      1. Vitai    
-- data_nascimento:  1. SMSRIO   | 2. Vitacare  | 3. Vitai
-- genero:           1. Vitacare | 2. SMSRIO    | 3. Vitai
-- raca:             1. Vitacare | 2. SMSRIO    | 3. Vitai
-- obito_indicador:  1. Vitacare | 2. SMSRIO    | 3. Vitai
-- obito_data:       1. Vitacare | 2. SMSRIO    | 3. Vitai
-- mae_nome:         1. SMSRIO   | 2. Vitacare  | 3. Vitai
-- pai_nome:         1. SMSRIO   | 2. Vitacare  | 3. Vitai

paciente_dados AS (
    SELECT 
        cpfs.cpf,
        STRUCT(
            CASE 
                WHEN sm.cpf IS NOT NULL THEN sm.nome
                WHEN vc.cpf IS NOT NULL THEN vc.nome
                WHEN vi.cpf IS NOT NULL THEN vi.nome
                ELSE NULL
            END AS nome,
            CASE 
                WHEN vc.cpf IS NOT NULL THEN vc.nome_social
                -- WHEN sm.cpf THEN sm.nome_social  -- SMSRIO não possui nome social
                -- WHEN vi.cpf IS NOT NULL THEN vi.nome_social  -- VITAI não possui nome social
                ELSE NULL
            END AS nome_social,
            CASE 
                WHEN sm.cpf IS NOT NULL THEN sm.data_nascimento
                WHEN vc.cpf IS NOT NULL THEN vc.data_nascimento
                WHEN vi.cpf IS NOT NULL THEN vi.data_nascimento
                ELSE NULL
            END AS data_nascimento,
            COALESCE(vc.genero, sm.genero, vi.genero) AS genero,
            COALESCE(vc.raca, sm.raca, vi.raca) AS raca,
            COALESCE(vc.obito_indicador, sm.obito_indicador, vi.obito_indicador) AS obito_indicador,
            COALESCE(vc.obito_data, sm.obito_data, vi.obito_data) AS obito_data,
            CASE 
                WHEN sm.cpf IS NOT NULL THEN sm.mae_nome
                WHEN vc.cpf IS NOT NULL THEN vc.mae_nome
                WHEN vi.cpf IS NOT NULL THEN vi.mae_nome
                ELSE NULL
            END AS mae_nome,
            CASE 
                WHEN sm.cpf IS NOT NULL THEN sm.pai_nome
                WHEN vc.cpf IS NOT NULL THEN vc.pai_nome
                WHEN vi.cpf IS NOT NULL THEN vi.pai_nome
                ELSE NULL
            END AS pai_nome,
            CASE 
                WHEN sm.cpf IS NOT NULL THEN TRUE
                ELSE FALSE 
            END AS identidade_validada_indicador
        ) AS dados
    FROM all_cpfs cpfs
    LEFT JOIN vitacare_tb vc ON cpfs.cpf = vc.cpf
    LEFT JOIN vitai_tb vi ON cpfs.cpf = vi.cpf
    LEFT JOIN smsrio_tb sm ON cpfs.cpf = sm.cpf 
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


SELECT 
    * 
FROM paciente_integrado