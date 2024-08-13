{{
    config(
        alias="paciente_vitai",
        materialized="table",
        schema="intermediario_historico_clinico"

    )
}}

-- This code integrates patient data from VITAI:
-- rj-sms.brutos_prontuario_vitai.paciente (VITAI)
-- The goal is to consolidate information such as registration data,
-- contact, address and medical record into a single view.

-- Declaration of the variable to filter by CPF (optional)
-- DECLARE cpf_filter STRING DEFAULT "";


---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
--  Get source data and standardize
---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--

-- Patient base table
WITH vitai_tb AS (
    SELECT 
        {{remove_accents_upper('cpf')}} AS cpf,
        {{remove_accents_upper('cns')}} AS cns,
        {{remove_accents_upper('nome')}} AS nome,
        {{remove_accents_upper('telefone')}} AS telefone,
        CAST("" AS STRING) AS email,
        CAST(NULL AS STRING) AS cep,
        {{remove_accents_upper('tipo_logradouro')}} AS tipo_logradouro,
        {{remove_accents_upper('nome_logradouro')}} AS logradouro,
        {{remove_accents_upper('numero')}} AS numero,
        {{remove_accents_upper('complemento')}} AS complemento,
        {{remove_accents_upper('bairro')}} AS bairro,
        {{remove_accents_upper('municipio')}} AS cidade,
        {{remove_accents_upper('uf')}} AS estado,
        {{remove_accents_upper('id_cidadao')}} AS id_paciente,
        {{remove_accents_upper('nome_alternativo')}} AS nome_social,
        {{remove_accents_upper('sexo')}} AS genero,
        {{remove_accents_upper('raca_cor')}} AS raca,
        {{remove_accents_upper('nome_mae')}} AS mae_nome,
        CAST(NULL AS STRING) AS pai_nome,
        DATE(data_nascimento) AS data_nascimento,
        DATE(data_obito) AS obito_data,
        updated_at,
        gid_estabelecimento AS id_cnes -- use gid to get id_cnes from  rj-sms.brutos_prontuario_vitai.estabelecimento
    FROM {{ref('raw_prontuario_vitai__paciente')}}
    WHERE cpf IS NOT NULL
        AND NOT REGEXP_CONTAINS(cpf, r'[A-Za-z]')
        AND TRIM(cpf) != ""
),

-- CNS
vitai_cns_ranked AS (
    SELECT
        cpf,
        cns,
        ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY updated_at DESC) AS rank
    FROM vitai_tb
    WHERE
        cns IS NOT NULL
        AND TRIM(cns) NOT IN ("")
    GROUP BY cpf, cns, updated_at
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
                3 AS merge_order
            FROM vitai_cns_ranked
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

-- CONTATO TELEPHONE
vitai_contato_telefone AS (
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
            ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY updated_at DESC) AS rank
        FROM vitai_tb
        GROUP BY cpf, telefone, updated_at
    )
    WHERE NOT (TRIM(valor) IN ("()", "") AND (rank >= 2))
),

-- CONTATO EMAIL
vitai_contato_email AS (
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
            ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY updated_at DESC) AS rank
        FROM vitai_tb
        GROUP BY cpf, email, updated_at
    )
    WHERE NOT (TRIM(valor) IN ("()", "") AND (rank >= 2))
),

contato_dados AS (
    SELECT
        COALESCE(vi_telefone.cpf, vi_email.cpf) AS cpf, 
        STRUCT(
            ARRAY_AGG(STRUCT(
                vi_telefone.valor AS valor, 
                vi_telefone.rank AS rank
            )) AS telefone,
            ARRAY_AGG(STRUCT(
                vi_email.valor  AS valor, 
                vi_email.rank AS rank
            )) AS email
        ) AS contato,
    FROM vitai_contato_telefone vi_telefone
    FULL OUTER JOIN vitai_contato_email vi_email
        ON vi_telefone.cpf = vi_email.cpf
    GROUP BY vi_telefone.cpf, vi_email.cpf
),


--  ENDEREÇO
vitai_endereco AS (
    SELECT
        cpf,
        cep,
        tipo_logradouro,
        CASE
            WHEN logradouro in ("NONE") THEN NULL
            ELSE logradouro
        END AS logradouro,
        numero,
        complemento,
        bairro,
        cidade,
        estado,
        CAST(updated_at AS STRING) AS datahora_ultima_atualizacao,
        ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY updated_at DESC) AS rank
    FROM vitai_tb
    WHERE logradouro IS NOT NULL
    GROUP BY
        cpf,cep, tipo_logradouro, logradouro, numero, complemento, bairro, cidade, estado, updated_at
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
            rank
        )) AS endereco
    FROM vitai_endereco 
    GROUP BY cpf
),

-- PRONTUARIO
vitai_prontuario AS (
    SELECT
        cpf,
        'VITAI' AS sistema,
        id_cnes,
        id_paciente,
        ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY updated_at DESC) AS rank
    FROM(
        SELECT 
            pc.updated_at,
            pc.cpf,
            pc.id_paciente,
            es.cnes AS id_cnes,
        FROM  vitai_tb pc
        JOIN  {{ ref('raw_prontuario_vitai__m_estabelecimento') }} es
            ON pc.id_cnes = es.gid
    )
    GROUP BY
        cpf, id_cnes, id_paciente, updated_at
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
    FROM vitai_prontuario
    GROUP BY cpf
),


-- PACIENTE DADOS
vitai_paciente AS (
    SELECT
        cpf,
        nome,
        nome_social,
        data_nascimento,
        CASE
            WHEN genero = "M" THEN "MALE"
            WHEN genero = "F" THEN "FEMALE"
            ELSE NULL
        END  AS genero,
        CASE
            WHEN raca IN ("None") THEN NULL
            WHEN raca IN ("PRETO","NEGRO") THEN "PRETA"
            WHEN raca = "NAO INFORMADO" THEN "SEM INFORMACAO"
            ELSE raca
        END AS raca,
        CASE
            WHEN obito_data IS NOT NULL THEN TRUE
            ELSE NULL
        END AS obito_indicador,
        obito_data,
        mae_nome,
        pai_nome,
        ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY updated_at) AS rank
    FROM vitai_tb
    GROUP BY
        cpf, pai_nome, nome, nome_social, data_nascimento, genero, obito_data, mae_nome, updated_at,
        CASE
            WHEN obito_data IS NOT NULL THEN TRUE
            ELSE NULL
        END,
        CASE
            WHEN raca IN ("None") THEN NULL
            WHEN raca IN ("PRETO","NEGRO") THEN "PRETA"
            WHEN raca = "NAO INFORMADO" THEN "SEM INFORMACAO"
            ELSE raca
        END
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
    FROM vitai_paciente
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
        ct.contato,
        ed.endereco,
        pt.prontuario,
        STRUCT(CURRENT_TIMESTAMP() AS created_at) AS metadados
    FROM paciente_dados pd
    LEFT JOIN cns_dados cns ON pd.cpf = cns.cpf
    LEFT JOIN contato_dados ct ON pd.cpf = ct.cpf
    LEFT JOIN endereco_dados ed ON pd.cpf = ed.cpf
    LEFT JOIN prontuario_dados pt ON pd.cpf = pt.cpf
)


SELECT * FROM paciente_integrado