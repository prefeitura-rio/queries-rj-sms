
WITH vitacare_tb AS (
    SELECT 
    TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(paciente_cpf, NFD), r'\pM', ''))) AS paciente_cpf,
    TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(cns, NFD), r'\pM', ''))) AS cns,
    TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(data_cadastro, NFD), r'\pM', ''))) AS data_cadastro,
    TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(cnes_unidade, NFD), r'\pM', ''))) AS cnes_unidade,
    TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(nome_unidade, NFD), r'\pM', ''))) AS nome_unidade,
    TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(data_atualizacao_vinculo_equipe, NFD), r'\pM', ''))) AS data_atualizacao_vinculo_equipe,
    TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(ine_equipe, NFD), r'\pM', ''))) AS ine_equipe,
    TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(telefone, NFD), r'\pM', ''))) AS telefone,
    TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(email, NFD), r'\pM', ''))) AS email,
    TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(cep, NFD), r'\pM', ''))) AS cep,
    TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(tipo_logradouro, NFD), r'\pM', ''))) AS tipo_logradouro,
    TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(logradouro, NFD), r'\pM', ''))) AS logradouro,
    TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(bairro, NFD), r'\pM', ''))) AS bairro,
    TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(municipio_residencia, NFD), r'\pM', ''))) AS municipio_residencia,
    TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(estado_residencia, NFD), r'\pM', ''))) AS estado_residencia,
    TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(id, NFD), r'\pM', ''))) AS id,
    TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(nome, NFD), r'\pM', ''))) AS nome,
    TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(nome_social, NFD), r'\pM', ''))) AS nome_social,
    TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(cpf, NFD), r'\pM', ''))) AS cpf,
    TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(data_nascimento, NFD), r'\pM', ''))) AS data_nascimento,
    TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(sexo, NFD), r'\pM', ''))) AS sexo,
    TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(raca_cor, NFD), r'\pM', ''))) AS raca_cor,
    TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(obito, NFD), r'\pM', ''))) AS obito,
    TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(nome_mae, NFD), r'\pM', ''))) AS nome_mae,
    TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(nome_pai, NFD), r'\pM', ''))) AS nome_pai
    FROM `rj-sms.brutos_prontuario_vitacare.paciente`
),

-- CNS

vitacare_cns_ranked AS (
    SELECT
        paciente_cpf,
        cns,
        ROW_NUMBER() OVER (PARTITION BY paciente_cpf ORDER BY cns DESC) AS rank
    FROM vitacare_tb
    WHERE
        cns IS NOT NULL
    GROUP BY paciente_cpf, cns
),

cns_dados AS (
    SELECT
        paciente_cpf,
        ARRAY_AGG(STRUCT(
            cns AS valor, 
            rank
        )) AS cns
    FROM vitacare_cns_ranked 
    GROUP BY paciente_cpf
),

-- CLINICA DA FAMILIA

vitacare_clinica_familia AS (
    SELECT
        paciente_cpf,
        cnes_unidade AS id_cnes,
        nome_unidade AS nome,
        data_atualizacao_vinculo_equipe AS datahora_ultima_atualizacao,
        ROW_NUMBER() OVER (PARTITION BY paciente_cpf ORDER BY data_atualizacao_vinculo_equipe DESC) AS rank
    FROM vitacare_tb
    WHERE
        nome_unidade IS NOT NULL
    GROUP BY
        paciente_cpf, cnes_unidade, nome_unidade, data_atualizacao_vinculo_equipe
),

clinica_familia_dados AS (
    SELECT
        paciente_cpf,
        ARRAY_AGG(STRUCT(
            id_cnes, 
            nome, 
            datahora_ultima_atualizacao, 
            rank
        )) AS clinica_familia
    FROM vitacare_clinica_familia
    GROUP BY paciente_cpf
),

-- EQUIPE SAUDE FAMILIA

vitacare_equipe_saude_familia AS (
    SELECT
        paciente_cpf,
        ine_equipe AS id_ine,
        data_atualizacao_vinculo_equipe AS datahora_ultima_atualizacao,
        ROW_NUMBER() OVER (PARTITION BY paciente_cpf ORDER BY data_atualizacao_vinculo_equipe DESC) AS rank
    FROM vitacare_tb
    WHERE
        ine_equipe IS NOT NULL
    GROUP BY
        paciente_cpf, ine_equipe, data_atualizacao_vinculo_equipe
),

equipe_saude_familia_dados AS (
    SELECT
        paciente_cpf,
        ARRAY_AGG(STRUCT(
            id_ine, 
            datahora_ultima_atualizacao, 
            rank
        )) AS equipe_saude_familia
    FROM vitacare_equipe_saude_familia
    GROUP BY paciente_cpf
),

-- EQUIPE CONTATO

vitacare_contato_telefone AS (
    SELECT 
        paciente_cpf,
        tipo, 
        CASE 
            WHEN TRIM(valor) IN ("()", "") THEN NULL
            ELSE valor
        END AS valor,
        rank
    FROM (
        SELECT
            paciente_cpf,
            'telefone' AS tipo,
            telefone AS valor,
            ROW_NUMBER() OVER (PARTITION BY paciente_cpf ORDER BY data_cadastro DESC) AS rank
        FROM vitacare_tb
        GROUP BY paciente_cpf, telefone, data_cadastro
    )
    WHERE NOT (TRIM(valor) IN ("()", "") AND (rank >= 2))
),

vitacare_contato_email AS (
    SELECT 
        paciente_cpf,
        tipo, 
        CASE 
            WHEN TRIM(valor) IN ("()", "") THEN NULL
            ELSE valor
        END AS valor,
        rank
    FROM (
        SELECT
            paciente_cpf,
            'email' AS tipo,
            email AS valor,
            ROW_NUMBER() OVER (PARTITION BY paciente_cpf ORDER BY data_cadastro DESC) AS rank
        FROM vitacare_tb
        GROUP BY paciente_cpf, email, data_cadastro
    )
    WHERE NOT (TRIM(valor) IN ("()", "") AND (rank >= 2))
),

contato_dados AS (
    SELECT
        COALESCE(ctt.paciente_cpf, cte.paciente_cpf) AS paciente_cpf,
        STRUCT(
            ARRAY_AGG(STRUCT(ctt.valor, ctt.rank)) AS telefone,
            ARRAY_AGG(STRUCT(cte.valor, cte.rank)) AS email
        ) AS contato
    FROM vitacare_contato_telefone ctt
    FULL OUTER JOIN vitacare_contato_email cte
        ON ctt.paciente_cpf = cte.paciente_cpf
    GROUP BY COALESCE(ctt.paciente_cpf, cte.paciente_cpf)
),

-- EQUIPE ENDEREÇO

vitacare_endereco AS (
    SELECT
        paciente_cpf,
        cep,
        tipo_logradouro,
        REGEXP_EXTRACT(logradouro, r'^(.*?)(?:\d+.*)?$') AS logradouro,
        REGEXP_EXTRACT(logradouro, r'\b(\d+)\b') AS numero,
        TRIM(REGEXP_REPLACE(logradouro, r'^.*?\d+\s*(.*)$', r'\1')) AS complemento,
        bairro,
        municipio_residencia AS cidade,
        estado_residencia AS estado,
        data_cadastro AS datahora_ultima_atualizacao,
        ROW_NUMBER() OVER (PARTITION BY paciente_cpf ORDER BY data_cadastro DESC) AS rank
    FROM vitacare_tb
    WHERE
        logradouro IS NOT NULL
    GROUP BY
        paciente_cpf, cep, tipo_logradouro, logradouro,REGEXP_EXTRACT(logradouro, r'\b(\d+)\b'),TRIM(REGEXP_REPLACE(logradouro, r'^.*?\d+\s*(.*)$', r'\1')), bairro, municipio_residencia, estado_residencia, data_cadastro
),

endereco_dados AS (
    SELECT
        paciente_cpf,
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
            rank
        )) AS endereco
    FROM vitacare_endereco
    GROUP BY paciente_cpf
),

-- PRONTUARIO

vitacare_prontuario AS (
    SELECT
        paciente_cpf,
        'VITACARE' AS sistema,
        cnes_unidade AS id_cnes,
        id AS id_paciente,
        ROW_NUMBER() OVER (PARTITION BY paciente_cpf ORDER BY data_cadastro DESC) AS rank
    FROM vitacare_tb
    GROUP BY
        paciente_cpf, cnes_unidade, id, data_cadastro
),

prontuario_dados AS (
    SELECT
        paciente_cpf,
        ARRAY_AGG(STRUCT(
            sistema, 
            id_cnes, 
            id_paciente, 
            rank
        )) AS prontuario
    FROM vitacare_prontuario
    GROUP BY paciente_cpf
),


-- PACIENTE DADOS

vitacare_paciente_dados AS (
    SELECT
        paciente_cpf,
        nome,
        nome_social,
        cpf,
        DATE(data_nascimento) AS data_nascimento,
        sexo AS genero,
        CASE
            WHEN TRIM(raca_cor) IN ("") THEN NULL
            ELSE raca_cor
        END AS raca,
        CASE
            WHEN obito = "FALSE" THEN FALSE
            WHEN obito = "TRUE" THEN TRUE
            ELSE NULL
        END AS obito_indicador,
        NULL AS obito_data,
        nome_mae AS mae_nome,
        nome_pai AS pai_nome,
        FALSE AS cadastro_validado_indicador,
        ROW_NUMBER() OVER (PARTITION BY paciente_cpf ORDER BY paciente_cpf) AS rank
    FROM vitacare_tb
    GROUP BY
        paciente_cpf, nome, nome_social, cpf, DATE(data_nascimento), sexo, raca_cor, obito, nome_mae, nome_pai
),

paciente_dados AS (
    SELECT
        paciente_cpf,
        ARRAY_AGG(STRUCT(
            nome,
            nome_social,
            cpf,
            data_nascimento,
            genero,
            raca,
            obito_indicador,
            obito_data,
            mae_nome,
            pai_nome,
            rank
        )) AS dados,
    FROM vitacare_paciente_dados
    GROUP BY
        paciente_cpf
)

-- FINAL JOIN

SELECT
    pd.paciente_cpf,
    cns.cns,
    pd.dados,
    cf.clinica_familia,
    esf.equipe_saude_familia,
    ct.contato,
    ed.endereco,
    pt.prontuario,
    STRUCT(CURRENT_TIMESTAMP() AS data_geracao) AS metadados
FROM paciente_dados pd
LEFT JOIN cns_dados cns ON pd.paciente_cpf = cns.paciente_cpf
LEFT JOIN clinica_familia_dados cf ON pd.paciente_cpf = cf.paciente_cpf
LEFT JOIN equipe_saude_familia_dados esf ON pd.paciente_cpf = esf.paciente_cpf
LEFT JOIN contato_dados ct ON pd.paciente_cpf = ct.paciente_cpf
LEFT JOIN endereco_dados ed ON pd.paciente_cpf = ed.paciente_cpf
LEFT JOIN prontuario_dados pt ON pd.paciente_cpf = pt.paciente_cpf