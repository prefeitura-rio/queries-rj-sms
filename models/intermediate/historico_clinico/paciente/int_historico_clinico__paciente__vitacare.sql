{{
    config(
        alias="paciente_vitacare",
        materialized="table",
        schema="intermediario_historico_clinico"
    )
}}

-- This code integrates patient data from VITACARE:
-- rj-sms.brutos_prontuario_vitacare.paciente (VITACARE)
-- The goal is to consolidate information such as registration data,
-- contact, address and medical record into a single view.

-- Declaration of the variable to filter by CPF (optional)
-- DECLARE cpf_filter STRING DEFAULT "";


---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
--  Get source data and standardize
---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--

-- Patient base table
WITH vitacare_tb AS (
SELECT
        {{remove_accents_upper('cpf')}} AS cpf,
        {{remove_accents_upper('cns')}} AS cns,
        {{remove_accents_upper('nome')}} AS nome,
        {{remove_accents_upper('cnes_unidade')}} AS id_cnes, -- use cnes_unidade to get name from  rj-sms.saude_dados_mestres.estabelecimento
        {{remove_accents_upper('codigo_ine_equipe_saude')}} AS id_ine,
        {{remove_accents_upper('telefone')}} AS telefone,
        {{remove_accents_upper('email')}} AS email,
        {{remove_accents_upper('endereco_cep')}} AS cep,
        {{remove_accents_upper('endereco_tipo_logradouro')}} AS tipo_logradouro,
        {{remove_accents_upper('REGEXP_EXTRACT(endereco_logradouro, r"^(.*?)(?:\d+.*)?$")')}} AS logradouro,
        {{remove_accents_upper('REGEXP_EXTRACT(endereco_logradouro, r"\b(\d+)\b")')}} AS numero,
        {{remove_accents_upper('REGEXP_REPLACE(endereco_logradouro, r"^.*?\d+\s*(.*)$", r"\1")')}} AS complemento,
        {{remove_accents_upper('endereco_bairro')}} AS bairro,
        {{remove_accents_upper('endereco_municipio')}} AS cidade,
        {{remove_accents_upper('endereco_estado')}} AS estado,
        {{remove_accents_upper('id')}} AS id_paciente,
        {{remove_accents_upper('nome_social')}} AS nome_social,
        {{remove_accents_upper('sexo')}} AS genero,
        {{remove_accents_upper('raca_cor')}} AS raca,
        {{remove_accents_upper('nome_mae')}} AS mae_nome,
        {{remove_accents_upper('nome_pai')}} AS pai_nome,
        DATE(data_obito) AS obito_data,
        DATE(data_nascimento) AS data_nascimento,
        data_atualizacao_vinculo_equipe, -- Change to data_atualizacao_vinculo_equipe
        updated_at,
        cadastro_permanente
    FROM {{ref('raw_prontuario_vitacare__paciente')}} -- `rj-sms-dev`.`brutos_prontuario_vitacare`.`paciente`
    WHERE cpf IS NOT NULL
        AND NOT REGEXP_CONTAINS(cpf, r'[A-Za-z]')
        AND TRIM(cpf) != ""
        AND tipo = "rotineiro"
        -- AND cpf = cpf_filter
),

-- CNS
vitacare_cns_ranked AS (
    SELECT
        cpf,
        cns,
        ROW_NUMBER() OVER (PARTITION BY cpf, cns ORDER BY data_atualizacao_vinculo_equipe DESC, cadastro_permanente DESC, updated_at DESC) AS rank,
    FROM vitacare_tb
    WHERE cns IS NOT NULL
    GROUP BY cpf, cns, data_atualizacao_vinculo_equipe, cadastro_permanente, updated_at
),

-- CNS Dados
cns_dedup AS (
    SELECT
        cpf,
        cns,
        ROW_NUMBER() OVER (PARTITION BY cpf  ORDER BY merge_order ASC, rank ASC) AS rank
    FROM(
        SELECT 
            cpf,
            cns,
            rank,
            merge_order,
            ROW_NUMBER() OVER (PARTITION BY cpf, cns ORDER BY merge_order, rank ASC) AS dedup_rank,
        FROM (
            SELECT 
                cpf,
                cns,
                rank,
                1 AS merge_order
            FROM vitacare_cns_ranked
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
                    rank
                )
        ) AS cns
    FROM cns_dedup
    GROUP BY cpf
),


-- CLINICA DA FAMILIA
vitacare_clinica_familia AS (
    SELECT
        vc.cpf,
        vc.id_cnes,
        e.nome_limpo AS nome,
        e.telefone,
        vc.data_atualizacao_vinculo_equipe,
        ROW_NUMBER() OVER (PARTITION BY vc.cpf ORDER BY vc.data_atualizacao_vinculo_equipe DESC, vc.cadastro_permanente DESC, vc.updated_at DESC) AS rank
    FROM vitacare_tb vc
    JOIN {{ ref("dim_estabelecimento") }} e
        ON vc.id_cnes = e.id_cnes
    GROUP BY
        vc.cpf, vc.id_cnes, e.nome_limpo, e.telefone, vc.data_atualizacao_vinculo_equipe, vc.cadastro_permanente, vc.updated_at
),

clinica_familia_dados AS (
    SELECT
        cpf,
        ARRAY_AGG(STRUCT(
            id_cnes, 
            nome,
            telefone,
            data_atualizacao_vinculo_equipe AS datahora_ultima_atualizacao,
            rank 
        )) AS clinica_familia
    FROM vitacare_clinica_familia
    GROUP BY cpf
),


-- EQUIPE SAUDE FAMILIA VITACARE: Extracts and ranks family health teams
-- EQUIPE SAUDE FAMILIA VITACARE: Extracts and ranks family health teams
vitacare_equipe_saude_familia AS (
    SELECT
        vc.cpf,
        vc.id_ine,
        e.nome_referencia AS nome,  
        e.telefone,
        ARRAY_AGG(STRUCT(medico_id AS id_profissional_sus, p.nome)) AS medicos, 
        ARRAY_AGG(STRUCT(enfermeiro_id AS id_profissional_sus, p2.nome)) AS enfermeiros, 
        data_atualizacao_vinculo_equipe AS datahora_ultima_atualizacao,
        ROW_NUMBER() OVER (PARTITION BY vc.cpf ORDER BY data_atualizacao_vinculo_equipe DESC, cadastro_permanente DESC, updated_at DESC) AS rank
    FROM vitacare_tb vc
    JOIN {{ ref("dim_equipe") }} e ON vc.id_ine = e.id_ine
    LEFT JOIN UNNEST(e.medicos) AS medico_id 
        ON TRUE  
    LEFT JOIN {{ ref("dim_profissional_saude") }} p 
        ON medico_id = p.id_profissional_sus
    LEFT JOIN UNNEST(e.enfermeiros) AS enfermeiro_id
        ON TRUE  
    LEFT JOIN {{ ref("dim_profissional_saude") }} p2 
        ON enfermeiro_id = p2.id_profissional_sus
    WHERE vc.id_ine IS NOT NULL
    GROUP BY vc.cpf, vc.id_ine, e.nome_referencia, e.telefone, data_atualizacao_vinculo_equipe, cadastro_permanente, updated_at
),

equipe_saude_familia_dados AS (
    SELECT
        cpf,
        ARRAY_AGG(STRUCT(
            id_ine, 
            nome,
            telefone,
            medicos,
            enfermeiros,
            datahora_ultima_atualizacao, 
            rank
        )) AS equipe_saude_familia
    FROM vitacare_equipe_saude_familia
    GROUP BY cpf
),


-- CONTATO TELEPHONE
vitacare_contato_telefone AS (
    SELECT 
        cpf,
        tipo, 
        CASE 
            WHEN TRIM(valor) IN ("()", "") THEN NULL
            ELSE valor
        END AS valor,
        rank
    FROM (
        SELECT
            cpf,
            'telefone' AS tipo,
            telefone AS valor,
            ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY data_atualizacao_vinculo_equipe DESC, cadastro_permanente DESC, updated_at DESC) AS rank
        FROM vitacare_tb
        GROUP BY cpf, telefone, data_atualizacao_vinculo_equipe, cadastro_permanente, updated_at
    )
    WHERE NOT (TRIM(valor) IN ("()", "") AND (rank >= 2))
),

-- CONTATO EMAIL
vitacare_contato_email AS (
    SELECT 
        cpf,
        tipo, 
        CASE 
            WHEN TRIM(valor) IN ("()", "") THEN NULL
            ELSE valor
        END AS valor,
        rank
    FROM (
        SELECT
            cpf,
            'email' AS tipo,
            email AS valor,
            ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY data_atualizacao_vinculo_equipe DESC, cadastro_permanente DESC, updated_at DESC) AS rank
        FROM vitacare_tb
        GROUP BY cpf, email, data_atualizacao_vinculo_equipe, cadastro_permanente, updated_at
    )
    WHERE NOT (TRIM(valor) IN ("()", "") AND (rank >= 2))
),

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
                valor, 
                rank,
                "VITACARE" AS sistema,
                1 AS merge_order
            FROM vitacare_contato_telefone
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
                valor, 
                rank,
                "VITACARE" AS sistema,
                1 AS merge_order
            FROM vitacare_contato_email
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


--  ENDEREÇO
vitacare_endereco AS (
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
        CAST(cadastro_permanente AS STRING) AS datahora_ultima_atualizacao,
        ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY data_atualizacao_vinculo_equipe DESC, cadastro_permanente DESC, updated_at DESC) AS rank
    FROM vitacare_tb
    WHERE
        logradouro IS NOT NULL
    GROUP BY
        cpf, cep, tipo_logradouro, logradouro,numero,complemento, bairro, cidade, estado, data_atualizacao_vinculo_equipe, cadastro_permanente, updated_at
),

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
                cep, 
                tipo_logradouro, 
                logradouro, 
                numero, 
                complemento, 
                bairro, 
                cidade, 
                estado, 
                datahora_ultima_atualizacao,
                rank,
                "VITACARE" AS sistema,
                1 AS merge_order
            FROM vitacare_endereco
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

-- PRONTUARIO
vitacare_prontuario AS (
    SELECT
        cpf,
        'VITACARE' AS sistema,
        id_cnes,
        id_paciente,
        ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY data_atualizacao_vinculo_equipe DESC, cadastro_permanente DESC, updated_at DESC) AS rank
    FROM vitacare_tb
    GROUP BY
        cpf, id_cnes, id_paciente, data_atualizacao_vinculo_equipe, cadastro_permanente, updated_at
),


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
                vi.cpf,
                "VITACARE" AS sistema,
                id_cnes, 
                id_paciente, 
                rank,
                1 AS merge_order
            FROM vitacare_prontuario vi
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


-- PACIENTE DADOS
vitacare_paciente AS (
    SELECT
        cpf,
        nome,
        CASE 
            WHEN nome_social IN ('') THEN NULL
            ELSE nome_social
        END AS nome_social,
        DATE(data_nascimento) AS data_nascimento,
        CASE
            WHEN genero IN ("M", "MALE") THEN "MASCULINO"
            WHEN genero IN ("F", "FEMALE") THEN "FEMININO"
            ELSE NULL
        END  AS genero,
        CASE
            WHEN TRIM(raca) IN ("") THEN NULL
            ELSE raca
        END AS raca,
        CASE
            WHEN obito_data IS NULL THEN FALSE
            WHEN obito_data IS NOT NULL THEN TRUE
            ELSE NULL
        END AS obito_indicador,
        obito_data AS obito_data,
        mae_nome,
        pai_nome,
        ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY data_atualizacao_vinculo_equipe DESC, cadastro_permanente DESC, updated_at DESC) AS rank
    FROM vitacare_tb
    GROUP BY cpf, nome, nome_social, cpf, data_nascimento, genero, raca, obito_data, mae_nome, pai_nome,updated_at, cadastro_permanente, data_atualizacao_vinculo_equipe
),


paciente_dados AS (
    SELECT
        cpf,
        ARRAY_AGG(STRUCT(
                nome,
                nome_social,
                data_nascimento,
                genero,
                raca,
                obito_indicador,
                obito_data,
                mae_nome,
                pai_nome,
                rank
        )) AS dados
    FROM vitacare_paciente
    GROUP BY cpf
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


SELECT * FROM paciente_integrado