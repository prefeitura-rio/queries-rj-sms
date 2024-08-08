-- This code integrates patient data from VITACARE:
-- rj-sms.brutos_prontuario_vitacare.paciente (VITACARE)
-- The goal is to consolidate information such as registration data,
-- contact, address and medical record into a single view.

-- Declaration of the variable to filter by CPF (optional)
-- DECLARE cpf_filter STRING DEFAULT "";

-- Auxiliary function to clean and standardize text fields
CREATE TEMP FUNCTION CleanText(texto STRING) AS (
    TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(texto, NFD), r'\pM', '')))
);

---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
--  Get source data and standardize
---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--

-- Patient base table
WITH vitacare_tb AS (
SELECT
        CleanText(cpf) AS cpf,
        CleanText(cns) AS cns,
        CleanText(nome) AS nome,
        CleanText(cnes_unidade) AS id_cnes, -- use cnes_unidade to get name from  rj-sms.saude_dados_mestres.estabelecimento
        CleanText(codigo_ine_equipe_saude) AS id_ine,
        CleanText(telefone) AS telefone,
        CleanText(email) AS email,
        CleanText(endereco_cep) AS cep,
        CleanText(endereco_tipo_logradouro) AS tipo_logradouro,
        CleanText(REGEXP_EXTRACT(endereco_logradouro, r'^(.*?)(?:\d+.*)?$')) AS logradouro,
        CleanText(REGEXP_EXTRACT(endereco_logradouro, r'\b(\d+)\b')) AS numero,
        CleanText(REGEXP_REPLACE(endereco_logradouro, r'^.*?\d+\s*(.*)$', r'\1')) AS complemento,
        CleanText(endereco_bairro) AS bairro,
        CleanText(endereco_municipio) AS cidade,
        CleanText(endereco_estado) AS estado,
        CleanText(id) AS id_paciente,
        CleanText(nome_social) AS nome_social,
        CleanText(sexo) AS genero,
        CleanText(raca_cor) AS raca,
        CleanText(nome_mae) AS mae_nome,
        CleanText(nome_pai) AS pai_nome,
        DATE(data_obito) AS obito_data,
        DATE(data_nascimento) AS data_nascimento,
        updated_at AS data_atualizacao_vinculo_equipe, -- Change to data_atualizacao_vinculo_equipe
        updated_at,
        cadastro_permanente,
    FROM `rj-sms.brutos_prontuario_vitacare.paciente`
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
        vc.data_atualizacao_vinculo_equipe,
        ROW_NUMBER() OVER (PARTITION BY vc.cpf ORDER BY vc.data_atualizacao_vinculo_equipe DESC, vc.cadastro_permanente DESC, vc.updated_at DESC) AS rank
    FROM vitacare_tb vc
    JOIN `rj-sms.saude_dados_mestres.estabelecimento` e
        ON vc.id_cnes = e.id_cnes
    GROUP BY
        vc.cpf, vc.id_cnes, e.nome_limpo, vc.data_atualizacao_vinculo_equipe, vc.cadastro_permanente, vc.updated_at
),

clinica_familia_dados AS (
    SELECT
        cpf,
        ARRAY_AGG(STRUCT(
            id_cnes, 
            nome, 
            data_atualizacao_vinculo_equipe AS datahora_ultima_atualizacao,
            rank 
        )) AS clinica_familia
    FROM vitacare_clinica_familia
    GROUP BY cpf
),


-- EQUIPE SAUDE FAMILIA VITACARE: Extracts and ranks family health teams
vitacare_equipe_saude_familia AS (
    SELECT
        cpf,
        id_ine,
        data_atualizacao_vinculo_equipe AS datahora_ultima_atualizacao,
        ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY data_atualizacao_vinculo_equipe DESC, cadastro_permanente DESC, updated_at DESC) AS rank
    FROM vitacare_tb
    WHERE
        id_ine IS NOT NULL
    GROUP BY
        cpf, id_ine, data_atualizacao_vinculo_equipe, cadastro_permanente, updated_at
),

equipe_saude_familia_dados AS (
    SELECT
        cpf,
        ARRAY_AGG(STRUCT(
            id_ine, 
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

contato_dados AS (
    SELECT
        COALESCE(vt_telefone.cpf, vt_email.cpf) AS cpf, 
        STRUCT(
            ARRAY_AGG(STRUCT(
                vt_telefone.valor AS valor, 
                vt_telefone.rank
            )) AS telefone,
            ARRAY_AGG(STRUCT(
                vt_email.valor  AS valor, 
                vt_email.rank
            )) AS email
        ) AS contato,
    FROM vitacare_contato_telefone vt_telefone
    FULL OUTER JOIN vitacare_contato_email vt_email
        ON vt_telefone.cpf = vt_email.cpf
    GROUP BY vt_telefone.cpf, vt_email.cpf
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

endereco_dados AS (
    SELECT
        cpf,
        ARRAY_AGG(STRUCT(
            cep AS cep, 
            tipo_logradouro AS tipo_logradouro, 
            logradouro AS logradouro, 
            numero AS numero, 
            complemento AS complemento, 
            bairro AS bairro, 
            cidade AS cidade, 
            estado AS estado, 
            datahora_ultima_atualizacao,
            rank
        )) AS endereco
    FROM vitacare_endereco 
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

prontuario_dados AS (
    SELECT
        cpf,
        ARRAY_AGG(STRUCT(
            sistema, 
            id_cnes, 
            id_paciente, 
            rank
        )) AS prontuario
    FROM vitacare_prontuario
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
        genero,
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
        ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY updated_at) AS rank
    FROM vitacare_tb
    GROUP BY cpf, nome, nome_social, cpf, data_nascimento, genero, raca, obito_data, mae_nome, pai_nome,updated_at
),


paciente_dados AS (
    SELECT
        cpf,
        STRUCT(
                nome,
                nome_social,
                data_nascimento,
                genero,
                raca,
                obito_indicador,
                obito_data,
                mae_nome,
                pai_nome
        ) AS dados
    FROM vitacare_paciente
    GROUP BY
        cpf, 
        nome,
        nome_social,
        cpf, 
        data_nascimento,
        genero,
        raca,
        obito_indicador, 
        obito_data,
        mae_nome, 
        pai_nome
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