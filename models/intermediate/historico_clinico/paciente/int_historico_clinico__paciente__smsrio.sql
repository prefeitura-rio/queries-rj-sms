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
        {{ validate_cpf(remove_accents_upper('cpf')) }} AS cpf_valido_indicador,
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
    FROM {{ref("raw_plataforma_smsrio__paciente")}} -- `rj-sms-dev`.`brutos_plataforma_smsrio`.`paciente`
    WHERE cpf IS NOT NULL
        AND NOT REGEXP_CONTAINS({{remove_accents_upper('cpf')}}, r'[A-Za-z]')
        AND TRIM({{remove_accents_upper('cpf')}}) NOT IN  ("","00000000000")
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
                "SMSRIO" AS sistema,
                2 AS merge_order
            FROM smsrio_contato_telefone
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
                "SMSRIO" AS sistema,
                2 AS merge_order
            FROM smsrio_contato_email
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
                "SMSRIO" AS sistema,
                2 AS merge_order
            FROM smsrio_endereco
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
                "SMSRIO" AS sistema,
                id_cnes, 
                id_paciente, 
                rank,
                2 AS merge_order
            FROM smsrio_prontuario vi
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
smsrio_paciente AS (
    SELECT
        cpf,
        cpf_valido_indicador,
        {{proper_br('nome')}} AS nome,
        {{proper_br('nome_social')}} AS nome_social,
        data_nascimento,
        CASE
            WHEN genero = "1" THEN INITCAP("MASCULINO")
            WHEN genero = "2" THEN INITCAP("FEMININO")
            ELSE NULL
        END  AS genero,
        CASE
            WHEN raca IN ("NONE", "None", "NAO INFORMADO", "SEM INFORMACAO") THEN NULL
            ELSE INITCAP(raca)
        END AS raca,
        CASE
            WHEN obito_indicador = "0" THEN FALSE
            WHEN obito_indicador = "1" THEN TRUE
            ELSE NULL
        END AS obito_indicador,
        obito_data,
        CASE
            WHEN mae_nome IN ("NONE") THEN NULL
            ELSE mae_nome
        END  AS mae_nome,
        pai_nome,
        ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY updated_at) AS rank
    FROM smsrio_tb
    GROUP BY
        cpf, nome,nome_social, cpf, data_nascimento, genero, obito_indicador, obito_data, mae_nome, pai_nome, updated_at,cpf_valido_indicador,
        CASE
            WHEN raca IN ("NONE", "None", "NAO INFORMADO", "SEM INFORMACAO") THEN NULL
            ELSE INITCAP(raca)
        END
),

paciente_metadados AS (
    SELECT
        cpf,
        STRUCT(
            -- count the distinct values for each field
            COUNT(DISTINCT nome) AS qtd_nomes,
            COUNT(DISTINCT nome_social) AS qtd_nomes_sociais,
            COUNT(DISTINCT data_nascimento) AS qtd_datas_nascimento,
            COUNT(DISTINCT genero) AS qtd_generos,
            COUNT(DISTINCT raca) AS qtd_racas,
            COUNT(DISTINCT obito_indicador) AS qtd_obitos_indicadores,
            COUNT(DISTINCT obito_data) AS qtd_datas_obitos,
            COUNT(DISTINCT mae_nome) AS qtd_maes_nomes,
            COUNT(DISTINCT pai_nome) AS qtd_pais_nomes,
            COUNT(DISTINCT cpf_valido_indicador) AS qtd_cpfs_validos,
            "SMSRIO" AS sistema
        ) AS metadados
    FROM smsrio_paciente
    GROUP BY cpf
),

paciente_dados AS (
    SELECT
        pc.cpf,
        ARRAY_AGG(STRUCT(
                cpf_valido_indicador,
                nome,
                nome_social,
                data_nascimento,
                genero,
                raca,
                obito_indicador,
                obito_data,
                mae_nome,
                pai_nome,
                rank,
                pm.metadados
        )) AS dados
    FROM smsrio_paciente pc
    JOIN paciente_metadados as pm
        ON pc.cpf = pm.cpf
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