
WITH vitai_tb AS (
    SELECT 
        TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(cpf, NFD), r'\pM', ''))) AS cpf,
        TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(cns, NFD), r'\pM', ''))) AS cns,
        TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(cliente, NFD), r'\pM', ''))) AS cliente,
        TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(nome, NFD), r'\pM', ''))) AS nome,
        TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(telefone, NFD), r'\pM', ''))) AS telefone,
        TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(tipo_logradouro, NFD), r'\pM', ''))) AS tipo_logradouro,
        TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(nome_logradouro, NFD), r'\pM', ''))) AS nome_logradouro,
        TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(numero, NFD), r'\pM', ''))) AS numero,
        TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(complemento, NFD), r'\pM', ''))) AS complemento,
        TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(bairro, NFD), r'\pM', ''))) AS bairro,
        TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(municipio, NFD), r'\pM', ''))) AS municipio,
        TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(uf, NFD), r'\pM', ''))) AS uf,
        updated_at,
        TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(id_cidadao, NFD), r'\pM', ''))) AS id_cidadao,
        TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(nome_alternativo, NFD), r'\pM', ''))) AS nome_alternativo,
        data_nascimento,
        TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(sexo, NFD), r'\pM', ''))) AS sexo,
        TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(raca_cor, NFD), r'\pM', ''))) AS raca_cor,
        data_obito,
        TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(nome_mae, NFD), r'\pM', ''))) AS nome_mae
    FROM `rj-sms.brutos_prontuario_vitai.paciente`
    WHERE cpf IS NOT NULL
        AND NOT REGEXP_CONTAINS(cpf, r'[A-Za-z]')
        AND TRIM(cpf) != ""
    LIMIT 100000
),

-- CNS

vitai_cns_ranked AS (
    SELECT
        cpf AS paciente_cpf,
        cns,
        ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY cpf DESC) AS rank
    FROM vitai_tb
    WHERE
        cns IS NOT NULL
        AND TRIM(cns) NOT IN ("")
    GROUP BY cpf, cns
),

cns_dados AS (
    SELECT
        paciente_cpf,
        ARRAY_AGG(STRUCT(
            cns AS valor, 
            rank
        )) AS cns
    FROM vitai_cns_ranked 
    GROUP BY paciente_cpf
),


-- EQUIPE CONTATO

vitai_contato AS (
    SELECT
        cpf AS paciente_cpf,
        'telefone' AS tipo,
        telefone AS valor,
        ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY cpf DESC) AS rank
    FROM vitai_tb
    GROUP BY
        cpf, telefone, updated_at
    UNION ALL
    SELECT
        cpf AS paciente_cpf,
        'email' AS tipo,
        "" AS valor, 
        ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY cpf DESC) AS rank
    FROM vitai_tb
    GROUP BY
        cpf
),


contato_dados AS (
    SELECT
        paciente_cpf,
        ARRAY_AGG(STRUCT(
            tipo, 
            CASE 
                WHEN TRIM(valor) IN ("()", "") THEN NULL
                ELSE valor
            END AS valor, 
            rank
        )) AS contato
    FROM vitai_contato
    WHERE NOT (TRIM(valor) IN ("()", "") AND (rank >= 2))
    GROUP BY paciente_cpf

),

-- EQUIPE ENDEREÇO

vitai_endereco AS (
    SELECT
        cpf AS paciente_cpf,
        NULL AS cep,
        tipo_logradouro,
        CASE
            WHEN nome_logradouro in ("NONE") THEN NULL
            ELSE nome_logradouro
        END AS logradouro,
        numero AS numero,
        complemento AS complemento,
        bairro,
        municipio AS cidade,
        uf AS estado,
        updated_at AS datahora_ultima_atualizacao,
        ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY updated_at DESC) AS rank
    FROM vitai_tb
    WHERE
        nome_logradouro IS NOT NULL
    GROUP BY
        cpf, tipo_logradouro, nome_logradouro, numero, complemento, bairro, municipio, uf, updated_at
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
    FROM vitai_endereco
    GROUP BY paciente_cpf
),

-- PRONTUARIO

vitai_prontuario AS (
    SELECT
        cpf AS paciente_cpf,
        'VITAI' AS sistema,
        cliente AS id_cnes,
        cpf AS id_paciente,
        ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY cpf DESC) AS rank
    FROM vitai_tb
    GROUP BY
        cpf, cliente
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
    FROM vitai_prontuario
    GROUP BY paciente_cpf
),

-- PACIENTE DADOS

vitai_paciente_dados AS (
    SELECT
        cpf AS paciente_cpf,
        nome,
        nome_alternativo AS nome_social,
        cpf,
        DATE(data_nascimento) AS data_nascimento,
        CASE
            WHEN sexo = "M" THEN "MALE"
            WHEN sexo = "F" THEN "FEMALE"
            ELSE NULL
        END  AS genero,
        CASE
            WHEN raca_cor IN ("None") THEN NULL
            WHEN raca_cor IN ("PRETO","NEGRO") THEN "PRETA"
            WHEN raca_cor = "NAO INFORMADO" THEN "SEM INFORMACAO"
            ELSE raca_cor
        END AS raca,
        CASE
            WHEN data_obito IS NOT NULL THEN TRUE
            ELSE NULL
        END AS obito_indicador,
        DATE(data_obito) AS obito_data,
        nome_mae AS mae_nome,
        NULL AS pai_nome,
        FALSE AS cadastro_validado_indicador,
        ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY cpf) AS rank
    FROM vitai_tb
    GROUP BY
        cpf, nome, nome_alternativo, cpf, DATE(data_nascimento), sexo, raca_cor, data_obito, nome_mae
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
    FROM vitai_paciente_dados
    GROUP BY
        paciente_cpf
)

-- FINAL JOIN

SELECT
    pd.paciente_cpf,
    cns.cns,
    pd.dados,
    ct.contato,
    ed.endereco,
    pt.prontuario,
    STRUCT(CURRENT_TIMESTAMP() AS data_geracao) AS metadados
FROM paciente_dados pd
LEFT JOIN cns_dados cns ON pd.paciente_cpf = cns.paciente_cpf
LEFT JOIN contato_dados ct ON pd.paciente_cpf = ct.paciente_cpf
LEFT JOIN endereco_dados ed ON pd.paciente_cpf = ed.paciente_cpf
LEFT JOIN prontuario_dados pt ON pd.paciente_cpf = pt.paciente_cpf