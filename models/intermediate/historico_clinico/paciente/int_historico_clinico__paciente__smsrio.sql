{{
    config(
        alias="paciente_smsrio",
        materialized="table",
        schema="intermediario_historico_clinico"
    )
}}

-- This code integrates patient data from SMSRIO:
-- rj-sms.brutos_plataforma_smsrio.paciente (SMSRIO)
-- The goal is to consolidate information such as registration data,
-- contact, address and medical record into a single view.

-- Declaration of the variable to filter by CPF (optional)
-- DECLARE cpf_filter STRING DEFAULT "";

-- SMSRIO: Patient base table
WITH smsrio_tb AS (
    SELECT 
        {{remove_accents_upper('cpf')}} AS cpf,
        {{remove_accents_upper('cns_lista')}} AS cns,
        {{remove_accents_upper('nome')}} AS nome,
        {{remove_accents_upper('telefone_lista')}} AS telefones,
        {{remove_accents_upper('email')}} AS email,
        {{remove_accents_upper('endereco_cep')}} AS cep,
        {{remove_accents_upper('endereco_tipo_logradouro')}} AS tipo_logradouro,
        {{remove_accents_upper('endereco_logradouro')}} AS logradouro,
        {{remove_accents_upper('endereco_numero')}} AS numero,
        {{remove_accents_upper('endereco_complemento')}} AS complemento,
        {{remove_accents_upper('endereco_bairro')}} AS bairro,
        {{remove_accents_upper('endereco_municipio_codigo')}} AS cidade,
        {{remove_accents_upper('endereco_uf')}} AS estado,
        {{remove_accents_upper('cpf')}} AS id_paciente,
        CAST(NULL AS STRING) AS nome_social,
        {{remove_accents_upper('sexo')}} AS genero,
        {{remove_accents_upper('raca_cor')}} AS raca,
        {{remove_accents_upper('nome_mae')}} AS mae_nome,
        {{remove_accents_upper('nome_pai')}} AS pai_nome,
        DATE(data_nascimento) AS data_nascimento,
        DATE(data_obito) AS obito_data,
        {{remove_accents_upper('obito')}} AS obito_indicador,
        updated_at,
        CAST(NULL AS STRING) AS id_cnes
    FROM {{ref("raw_plataforma_smsrio__paciente")}}
    WHERE cpf IS NOT NULL
        AND NOT REGEXP_CONTAINS(cpf, r'[A-Za-z]')
        AND TRIM(cpf) != ""
),


-- CNS
smsrio_cns_ranked AS (
    SELECT
        cpf,
        CASE 
            WHEN TRIM(cns) IN ('NONE') THEN NULL
            ELSE TRIM(cns)
        END AS cns,
        ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY updated_at DESC) AS rank,
    FROM (
            SELECT
                cpf,
                cns,
                updated_at
            FROM smsrio_tb,
            UNNEST(SPLIT(REPLACE(REPLACE(REPLACE(cns, '[', ''), ']', ''), '"', ''), ',')) AS cns
    )
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
                2 AS merge_order
            FROM smsrio_cns_ranked
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
smsrio_contato_telefone AS (
    SELECT 
        cpf,
        tipo, 
        CASE 
            WHEN TRIM(valor) IN ("NONE", "NULL", "") THEN NULL
            ELSE valor
        END AS valor,
        rank
    FROM (
        SELECT
            cpf,
            'telefone' AS tipo,
            TRIM(telefones) AS valor,
            ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY updated_at DESC) AS rank
        FROM (
            SELECT
                cpf,
                telefones,
                updated_at
            FROM smsrio_tb,
            UNNEST(SPLIT(REPLACE(REPLACE(REPLACE(telefones, '[', ''), ']', ''), '"', ''), ',')) AS telefones
        )
        GROUP BY
            cpf, telefones, updated_at
    )
    WHERE NOT (TRIM(valor) IN ("NONE", "NULL", "") AND (rank >= 2))
),

-- CONTATO SMSRIO: Extracts and ranks email
smsrio_contato_email AS (
    SELECT 
        cpf,
        tipo, 
        CASE 
            WHEN TRIM(valor) IN ("NONE", "NULL", "") THEN NULL
            ELSE valor
        END AS valor,
        rank
    FROM (
        SELECT
            cpf,
            'email' AS tipo,
            email AS valor,
            ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY updated_at DESC) AS rank
        FROM smsrio_tb
        GROUP BY
            cpf, email, updated_at
    )
    WHERE NOT (TRIM(valor) IN ("NONE", "NULL", "") AND (rank >= 2))
),

contato_dados AS (
    SELECT
        COALESCE(sms_telefone.cpf, sms_email.cpf) AS cpf, 
        STRUCT(
            ARRAY_AGG(STRUCT(
                sms_telefone.valor AS valor, 
                sms_telefone.rank AS rank
            )) AS telefone,
            ARRAY_AGG(STRUCT(
                sms_email.valor  AS valor, 
                sms_email.rank AS rank
            )) AS email
        ) AS contato,
    FROM smsrio_contato_telefone sms_telefone
    FULL OUTER JOIN smsrio_contato_email sms_email
        ON sms_telefone.cpf = sms_email.cpf
    GROUP BY sms_telefone.cpf, sms_email.cpf
),


--  ENDEREÇO
smsrio_endereco AS (
    SELECT
        cpf,
        cep,
        CASE 
            WHEN tipo_logradouro IN ("NONE","") THEN NULL
            ELSE tipo_logradouro
        END AS tipo_logradouro,
        logradouro,
        numero,
        complemento,
        bairro,
        CASE 
            WHEN cidade IN ("NONE","") THEN NULL
            ELSE cidade
        END AS cidade,
        estado,
        CAST(updated_at AS STRING) AS datahora_ultima_atualizacao,
        ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY updated_at DESC) AS rank
    FROM smsrio_tb
    GROUP BY
        cpf, cep, tipo_logradouro, logradouro, numero, complemento, bairro, cidade, estado, updated_at
),

endereco_dados AS (
    SELECT
        cpf,
        ARRAY_AGG(STRUCT(
            cep AS cep, 
            tipo_logradouro, 
            logradouro, 
            numero, 
            complemento, 
            bairro, 
            cidade, 
            estado, 
            datahora_ultima_atualizacao,
            rank AS rank
        )) AS endereco
    FROM smsrio_endereco 
    GROUP BY cpf
),

-- PRONTUARIO
smsrio_prontuario AS (
    SELECT
        cpf,
        'SMSRIO' AS sistema,
        id_cnes,
        id_paciente,
        ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY updated_at DESC) AS rank
    FROM smsrio_tb
    GROUP BY
        cpf, id_cnes,id_paciente, updated_at
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
    FROM smsrio_prontuario
    GROUP BY cpf
),


-- PACIENTE DADOS
smsrio_paciente AS (
    SELECT
        cpf,
        nome,
        nome_social,
        data_nascimento,
        CASE
            WHEN genero = "1" THEN "MALE"
            WHEN genero = "2" THEN "FEMALE"
        ELSE NULL
        END  AS genero,
        CASE
            WHEN raca IN ("None") THEN NULL
        ELSE raca
        END AS raca,
        CASE
            WHEN obito_indicador = "0" THEN FALSE
            WHEN obito_indicador = "1" THEN TRUE
        ELSE NULL
        END AS obito_indicador,
        obito_data,
        mae_nome,
        pai_nome,
        ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY updated_at) AS rank
    FROM smsrio_tb
    GROUP BY
        cpf, nome,nome_social, cpf, data_nascimento, genero, raca, obito_indicador, obito_data, mae_nome, pai_nome, updated_at
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
    FROM smsrio_paciente
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