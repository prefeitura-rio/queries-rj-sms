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
        {{ validate_cpf(remove_accents_upper('cpf')) }} AS cpf_valido_indicador,
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
        AND NOT REGEXP_CONTAINS({{remove_accents_upper('cpf')}}, r'[A-Za-z]')
        AND TRIM({{remove_accents_upper('cpf')}}) != ""
        -- AND tipo = "rotineiro"
        -- AND cpf = cpf_filter
),

-- CNS
vitacare_cns_ranked AS (
    SELECT
        cpf,
        cns,
        ROW_NUMBER() OVER (PARTITION BY cpf, cns ORDER BY data_atualizacao_vinculo_equipe DESC, cadastro_permanente DESC, updated_at DESC) AS rank,
    FROM (
        SELECT 
            cpf,
            CASE 
                WHEN TRIM(cns) IN ('NONE') THEN NULL
                ELSE TRIM(cns)
            END AS cns,
            data_atualizacao_vinculo_equipe,
            cadastro_permanente,
            updated_at
        FROM vitacare_tb
    )
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


cns_validated AS (
    SELECT
        cns,
        {{validate_cns('cns')}} AS cns_valido_indicador,
    FROM (
        SELECT DISTINCT cns FROM cns_dedup
    )
),

cns_dados AS (
    SELECT 
        cpf,
        ARRAY_AGG(
                STRUCT(
                    cd.cns, 
                    cv.cns_valido_indicador,
                    cd.rank
                )
        ) AS cns
    FROM cns_dedup cd
    JOIN cns_validated cv
        ON cd.cns = cv.cns
    GROUP BY cpf
),

-- EQUIPE SAUDE FAMILIA VITACARE: Extracts and ranks family health teams


-- clinica da familia
vitacare_clinica_familia AS (
    SELECT
        vc.cpf,
        vc.id_cnes,
        {{proper_estabelecimento('e.nome_limpo')}} AS nome,
        IF(ARRAY_LENGTH( e.telefone) > 0,  e.telefone[OFFSET(0)], NULL) AS telefone,
        vc.data_atualizacao_vinculo_equipe,
        ROW_NUMBER() OVER (PARTITION BY vc.cpf ORDER BY vc.data_atualizacao_vinculo_equipe DESC, vc.cadastro_permanente DESC, vc.updated_at DESC) AS rank
    FROM vitacare_tb vc
    JOIN {{ ref("dim_estabelecimento") }} e
        ON vc.id_cnes = e.id_cnes
    WHERE vc.id_cnes IS NOT NULL
    GROUP BY
        vc.cpf, vc.id_cnes, vc.data_atualizacao_vinculo_equipe, vc.cadastro_permanente, vc.updated_at, e.nome_limpo,  
        IF(ARRAY_LENGTH( e.telefone) > 0,  e.telefone[OFFSET(0)], NULL)
        
),

-- medicos data
medicos_data AS (
    SELECT
        e.id_ine,
        ARRAY_AGG(STRUCT(
            p.id_profissional_sus, 
            {{proper_br('p.nome')}} AS nome
        )) AS medicos
    FROM {{ ref("dim_equipe") }} e -- `rj-sms-dev`.`saude_dados_mestres`.`equipe_profissional_saude` 
    LEFT JOIN UNNEST(e.medicos) AS medico_id
    LEFT JOIN {{ ref("dim_profissional_saude") }} p -- `rj-sms-dev`.`saude_dados_mestres`.`profissional_saude`
        ON medico_id = p.id_profissional_sus
    GROUP BY e.id_ine
),

-- enfermeiros data
enfermeiros_data AS (
    SELECT
        e.id_ine,
        ARRAY_AGG(STRUCT(
            p.id_profissional_sus,
            {{proper_br('p.nome')}} AS nome
        )) AS enfermeiros
    FROM {{ ref("dim_equipe") }} e -- `rj-sms-dev`.`saude_dados_mestres`.`equipe_profissional_saude` 
    LEFT JOIN UNNEST(e.enfermeiros) AS enfermeiro_id
    LEFT JOIN {{ ref("dim_profissional_saude") }} p -- `rj-sms-dev`.`saude_dados_mestres`.`profissional_saude`
        ON enfermeiro_id = p.id_profissional_sus
    GROUP BY e.id_ine
),

vitacare_equipe_saude_familia AS (
    SELECT
        vc.cpf,
        vc.id_ine,
        {{proper_br('e.nome_referencia')}} AS nome,
        e.telefone,
        m.medicos, 
        en.enfermeiros, 
        vc.data_atualizacao_vinculo_equipe AS datahora_ultima_atualizacao,
        ROW_NUMBER() OVER (PARTITION BY vc.cpf ORDER BY vc.data_atualizacao_vinculo_equipe DESC, vc.cadastro_permanente DESC, vc.updated_at DESC) AS rank
    FROM vitacare_tb vc
    JOIN {{ ref("dim_equipe") }} e -- `rj-sms-dev`.`saude_dados_mestres`.`equipe_profissional_saude` 
        ON vc.id_ine = e.id_ine
    LEFT JOIN medicos_data m
        ON vc.id_ine = m.id_ine
    LEFT JOIN enfermeiros_data en
        ON vc.id_ine = en.id_ine
    WHERE vc.id_ine IS NOT NULL
    GROUP BY vc.cpf, vc.id_ine, e.telefone, m.medicos, en.enfermeiros, vc.data_atualizacao_vinculo_equipe, vc.cadastro_permanente, vc.updated_at, e.nome_referencia
),

equipe_saude_familia_dados AS (
    SELECT
        ef.cpf,
        ARRAY_AGG(STRUCT(
            ef.id_ine, 
            ef.nome,
            ef.telefone,
            ef.medicos,
            ef.enfermeiros,
            STRUCT(
                cf.id_cnes, 
                cf.nome,
                cf.telefone
            ) AS clinica_familia,
            ef.datahora_ultima_atualizacao, 
            ef.rank
        )) AS equipe_saude_familia
    FROM vitacare_equipe_saude_familia ef
    LEFT JOIN vitacare_clinica_familia cf
        ON ef.cpf = cf.cpf
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
        CAST(data_atualizacao_vinculo_equipe AS STRING) AS datahora_ultima_atualizacao,
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
        cpf_valido_indicador,
        {{proper_br('nome')}} AS nome,
        CASE 
            WHEN nome_social IN ('') THEN NULL
            ELSE {{proper_br('nome_social')}}
        END AS nome_social,
        DATE(data_nascimento) AS data_nascimento,
        CASE
            WHEN genero IN ("M", "MALE") THEN INITCAP("MASCULINO")
            WHEN genero IN ("F", "FEMALE") THEN INITCAP("FEMININO")
            ELSE NULL
        END  AS genero,
        CASE
            WHEN TRIM(raca) IN ("", "NAO INFORMADO", "SEM INFORMACAO") THEN NULL
            ELSE INITCAP(raca)
        END AS raca,
        CASE
            WHEN obito_data IS NULL THEN FALSE
            WHEN obito_data IS NOT NULL THEN TRUE
            ELSE NULL
        END AS obito_indicador,
        obito_data AS obito_data,
        CASE
            WHEN mae_nome IN ("NONE") THEN NULL
            ELSE mae_nome
        END  AS mae_nome,
        pai_nome,
        ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY data_atualizacao_vinculo_equipe DESC, cadastro_permanente DESC, updated_at DESC) AS rank
    FROM vitacare_tb
    GROUP BY cpf, nome, nome_social, cpf, data_nascimento, genero, raca, obito_data, mae_nome, pai_nome,updated_at, cadastro_permanente, data_atualizacao_vinculo_equipe, cpf_valido_indicador
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
            "VITACARE" AS sistema
        ) AS metadados
    FROM vitacare_paciente
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
    FROM vitacare_paciente pc
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
        esf.equipe_saude_familia,
        ct.contato,
        ed.endereco,
        pt.prontuario,
        STRUCT(CURRENT_TIMESTAMP() AS created_at) AS metadados
    FROM paciente_dados pd
    LEFT JOIN cns_dados cns ON pd.cpf = cns.cpf
    LEFT JOIN equipe_saude_familia_dados esf ON pd.cpf = esf.cpf
    LEFT JOIN contato_dados ct ON pd.cpf = ct.cpf
    LEFT JOIN endereco_dados ed ON pd.cpf = ed.cpf
    LEFT JOIN prontuario_dados pt ON pd.cpf = pt.cpf
)


SELECT * FROM paciente_integrado