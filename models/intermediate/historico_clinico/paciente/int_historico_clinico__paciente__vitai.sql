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
    FROM {{ref('raw_prontuario_vitai__paciente')}} -- `rj-sms-dev`.`brutos_prontuario_vitai`.`paciente`
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
                "VITAI" AS sistema,
                3 AS merge_order
            FROM vitai_contato_telefone
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
                "VITAI" AS sistema,
                3 AS merge_order
            FROM vitai_contato_email
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
vitai_endereco AS (
    SELECT
        cpf,
        CASE
            WHEN cep in ("NONE") THEN NULL
            ELSE cep
        END AS cep,
        CASE
            WHEN tipo_logradouro in ("NONE") THEN NULL
            ELSE tipo_logradouro
        END AS tipo_logradouro,
        CASE
            WHEN logradouro in ("NONE") THEN NULL
            ELSE logradouro
        END AS logradouro,
        CASE
            WHEN numero in ("NONE") THEN NULL
            ELSE numero
        END AS numero,
        CASE
            WHEN complemento in ("NONE") THEN NULL
            ELSE complemento
        END AS complemento,
        CASE
            WHEN bairro in ("NONE") THEN NULL
            ELSE bairro
        END AS bairro,
        CASE
            WHEN cidade in ("NONE") THEN NULL
            ELSE cidade
        END AS cidade,
        CASE
            WHEN estado in ("NONE") THEN NULL
            ELSE estado
        END AS estado,
        CAST(updated_at AS STRING) AS datahora_ultima_atualizacao,
        ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY updated_at DESC) AS rank
    FROM vitai_tb
    WHERE logradouro IS NOT NULL
    GROUP BY
        cpf,cep, tipo_logradouro, logradouro, numero, complemento, bairro, cidade, estado, updated_at
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
                "VITAI" AS sistema,
                3 AS merge_order
            FROM vitai_endereco
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
                "VITAI" AS sistema,
                id_cnes, 
                id_paciente, 
                rank,
                3 AS merge_order
            FROM vitai_prontuario vi
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
vitai_paciente AS (
    SELECT
        cpf,
        nome,
        nome_social,
        data_nascimento,
        CASE
            WHEN genero = "M" THEN "MASCULINO"
            WHEN genero = "F" THEN "FEMININO"
            ELSE NULL
        END  AS genero,
        CASE
            WHEN raca IN ("NONE") THEN NULL
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
            WHEN raca IN ("NONE") THEN NULL
            WHEN raca IN ("PRETO","NEGRO") THEN "PRETA"
            WHEN raca = "NAO INFORMADO" THEN "SEM INFORMACAO"
            ELSE raca
        END
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
    FROM vitai_paciente
    GROUP BY cpf
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