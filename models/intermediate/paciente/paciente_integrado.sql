-- This code integrates patient data from two sources:
-- rj-sms.brutos_prontuario_vitacare.paciente (VITACARE)
-- rj-sms.brutos_plataforma_vitai.paciente (VITAI)
-- rj-sms.brutos_plataforma_smsrio.paciente (SMSRIO)
-- The goal is to consolidate information such as registration data,
-- contact, address and medical record into a single view.

-- Auxiliary function to clean and standardize text fields
CREATE TEMP FUNCTION CleanText(texto STRING) AS (
    TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(texto, NFD), r'\pM', '')))
);

-- Declaration of the variable to filter by CPF (optional)
--DECLARE cpf_filter STRING DEFAULT "";

---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
--  Get source data and standardize
---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--

-- VITACARE: Patient base table
WITH vitacare_tb AS (
    SELECT
        CleanText(paciente_cpf) AS paciente_cpf,
        CleanText(cns) AS cns,
        data_cadastro,
        CleanText(cnes_unidade) AS cnes_unidade,
        CleanText(nome_unidade) AS nome_unidade,
        data_atualizacao_vinculo_equipe,
        CleanText(ine_equipe) AS ine_equipe,
        CleanText(telefone) AS telefone,
        CleanText(email) AS email,
        CleanText(cep) AS cep,
        CleanText(tipo_logradouro) AS tipo_logradouro,
        CleanText(logradouro) AS logradouro,
        CleanText(bairro) AS bairro,
        CleanText(municipio_residencia) AS municipio_residencia,
        CleanText(estado_residencia) AS estado_residencia,
        CleanText(id) AS id,
        CleanText(nome) AS nome,
        CleanText(nome_social) AS nome_social,
        CleanText(cpf) AS cpf,
        CleanText(data_nascimento) AS data_nascimento,
        CleanText(sexo) AS sexo,
        CleanText(raca_cor) AS raca_cor,
        CleanText(obito) AS obito,
        CleanText(nome_mae) AS nome_mae,
        CleanText(nome_pai) AS nome_pai,
        updated_at
    FROM `rj-sms.brutos_prontuario_vitacare.paciente`
    -- WHERE paciente_cpf = cpf_filter
),

-- SMSRIO: Patient base table
smsrio_tb AS (
    SELECT 
        CleanText(paciente_cpf) AS paciente_cpf,
        CleanText(cns_provisorio) AS cns_provisorio,
        CleanText(telefones) AS telefones,
        CleanText(email) AS email,
        timestamp,
        CleanText(end_logrado) AS end_logrado,
        CleanText(end_cep) AS end_cep,
        CleanText(end_tp_logrado_cod) AS end_tp_logrado_cod,
        CleanText(end_numero) AS end_numero,
        CleanText(end_complem) AS end_complem,
        CleanText(end_bairro) AS end_bairro,
        CleanText(cod_mun_res) AS cod_mun_res,
        CleanText(uf_res) AS uf_res,
        dt_nasc,
        CleanText(sexo) AS sexo,
        CleanText(raca_cor) AS raca_cor,
        CleanText(obito) AS obito,
        dt_obito,
        CleanText(nome_mae) AS nome_mae,
        CleanText(nome_pai) AS nome_pai,
        CleanText(nome) AS nome,
        updated_at
    FROM `rj-sms.brutos_plataforma_smsrio.paciente`
    -- WHERE paciente_cpf = cpf_filter
),

---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
--  Get CNS from the source tables
-- giving preference to the most recently registered.
---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--

-- CNS VITACARE: Extracts and ranks CNS numbers
vitacare_cns_ranked AS (
    SELECT
        paciente_cpf,
        cns,
        ROW_NUMBER() OVER (PARTITION BY paciente_cpf ORDER BY updated_at DESC) AS rank
    FROM vitacare_tb
    WHERE
        cns IS NOT NULL
    GROUP BY paciente_cpf, cns, updated_at
),

-- CNS SMSRIO: Extracts and ranks CNS numbers 
smsrio_cns_ranked AS (
    SELECT
        paciente_cpf,
        TRIM(cns) AS cns,
        ROW_NUMBER() OVER (PARTITION BY paciente_cpf ORDER BY updated_at DESC) AS rank
    FROM (
            SELECT
                paciente_cpf,
                cns,
                updated_at
            FROM smsrio_tb,
            UNNEST(SPLIT(REPLACE(REPLACE(REPLACE(cns_provisorio, '[', ''), ']', ''), '"', ''), ',')) AS cns
    )
    GROUP BY paciente_cpf, TRIM(cns), updated_at
    
),

---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
--  Get Family Clinic data
--  attended by each patient based on the most recent visit date.
---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--

-- CLINICA DA FAMILIA VITACARE: Extracts and ranks family clinics
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

---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
--  Get Family Health Team data
--  attended by each patient based on the most recent visit date.
---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--

-- EQUIPE SAUDE FAMILIA VITACARE: Extracts and ranks family health teams
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

---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
--  Get contact information
--  prioritizing the most recently registered.
---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--

-- CONTATO VITACARE: Extracts and ranks telephone numbers
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

-- CONTATO VITACARE: Extracts and ranks email
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

-- CONTATO SMSRIO: Extracts and ranks phone numbers
smsrio_contato_telefone AS (
    SELECT 
        paciente_cpf,
        tipo, 
        CASE 
            WHEN TRIM(valor) IN ("NONE", "NULL", "") THEN NULL
            ELSE valor
        END AS valor,
        rank
    FROM (
        SELECT
            paciente_cpf,
            'telefone' AS tipo,
            TRIM(telefones) AS valor,
            ROW_NUMBER() OVER (PARTITION BY paciente_cpf ORDER BY updated_at DESC) AS rank
        FROM (
            SELECT
                paciente_cpf,
                telefones,
                updated_at
            FROM smsrio_tb,
            UNNEST(SPLIT(REPLACE(REPLACE(REPLACE(telefones, '[', ''), ']', ''), '"', ''), ',')) AS telefones
        )
        GROUP BY
            paciente_cpf, telefones, updated_at
    )
    WHERE NOT (TRIM(valor) IN ("NONE", "NULL", "") AND (rank >= 2))
),

-- CONTATO SMSRIO: Extracts and ranks email
smsrio_contato_email AS (
    SELECT 
        paciente_cpf,
        tipo, 
        CASE 
            WHEN TRIM(valor) IN ("NONE", "NULL", "") THEN NULL
            ELSE valor
        END AS valor,
        rank
    FROM (
        SELECT
            paciente_cpf,
            'email' AS tipo,
            email AS valor,
            ROW_NUMBER() OVER (PARTITION BY paciente_cpf ORDER BY updated_at DESC) AS rank
        FROM smsrio_tb
        GROUP BY
            paciente_cpf, email, updated_at
    )
    WHERE NOT (TRIM(valor) IN ("NONE", "NULL", "") AND (rank >= 2))
),

---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
--  Get address information
--  prioritizing the most recent by registration date.
---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--

--  ENDEREÇO VITACARE: Extracts and ranks addresses, 
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

--  ENDEREÇO SMSRIO: Extracts and ranks addresses
smsrio_endereco AS (
    SELECT
        paciente_cpf,
        end_cep AS cep,
        CASE 
            WHEN end_tp_logrado_cod IN ("NONE","") THEN NULL
            ELSE end_tp_logrado_cod
        END AS tipo_logradouro,
        end_logrado AS logradouro,
        end_numero AS numero,
        end_complem AS complemento,
        end_bairro AS bairro,
        CASE 
            WHEN cod_mun_res IN ("NONE","") THEN NULL
            ELSE cod_mun_res
        END AS cidade,
        uf_res AS estado,
        updated_at AS datahora_ultima_atualizacao,
        ROW_NUMBER() OVER (PARTITION BY paciente_cpf ORDER BY updated_at DESC) AS rank
    FROM smsrio_tb
    GROUP BY
        paciente_cpf, end_cep, end_tp_logrado_cod, end_logrado, end_numero, end_complem, end_bairro, cod_mun_res, uf_res, updated_at
),

---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
--  Get system medical record information
--  prioritizing the most recently registered.
---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--

-- PRONTUARIO: Extracts and ranks medical record information
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
-- PRONTUARIO SMSRIO: Extracts and ranks medical record information
smsrio_prontuario AS (
    SELECT
        paciente_cpf,
        'SMSRIO' AS sistema,
        NULL AS id_cnes,
        paciente_cpf AS id_paciente,
        ROW_NUMBER() OVER (PARTITION BY paciente_cpf ORDER BY updated_at DESC) AS rank
    FROM smsrio_tb
    GROUP BY
        paciente_cpf, cod_mun_res, updated_at
),


---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
--  Get and structure patient data
---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--

-- PACIENTE DADOS VITACARE: Extracts and structures patient data
vitacare_paciente AS (
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
        DATE(NULL) AS obito_data,
        nome_mae AS mae_nome,
        nome_pai AS pai_nome,
        FALSE AS cadastro_validado_indicador,
        ROW_NUMBER() OVER (PARTITION BY paciente_cpf ORDER BY updated_at) AS rank
    FROM vitacare_tb
    GROUP BY
        paciente_cpf, nome, nome_social, cpf, DATE(data_nascimento), sexo, raca_cor, obito, nome_mae, nome_pai,updated_at
),

-- PACIENTE DADOS SMSRIO: Extracts and structures patient data
smsrio_paciente AS (
    SELECT
        paciente_cpf,
        nome,
        CAST(NULL AS STRING) AS nome_social,
        paciente_cpf AS cpf,
        DATE(dt_nasc) AS data_nascimento,
        CASE
            WHEN sexo = "1" THEN "MALE"
            WHEN sexo = "2" THEN "FEMALE"
        ELSE NULL
        END  AS genero,
        CASE
            WHEN raca_cor IN ("None") THEN NULL
        ELSE raca_cor
        END AS raca,
        CASE
            WHEN obito = "0" THEN FALSE
            WHEN obito = "1" THEN TRUE
        ELSE NULL
        END AS obito_indicador,
        CASE
            WHEN dt_obito IN ("None") THEN NULL
            ELSE DATE(dt_obito)
        END AS obito_data,
        nome_mae AS mae_nome,
        nome_pai AS pai_nome,
        TRUE AS cadastro_validado_indicador,
        ROW_NUMBER() OVER (PARTITION BY paciente_cpf ORDER BY paciente_cpf) AS rank
    FROM smsrio_tb
    GROUP BY
        paciente_cpf, nome, DATE(dt_nasc), sexo, raca_cor, obito, dt_obito, nome_mae, nome_pai
),

---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
--  Merge data from different sources
---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--

-- CNS Dados: Merges CNS data, grouping by patient.
cns_dados AS (
    SELECT
        COALESCE(sm.paciente_cpf,vc.paciente_cpf) AS paciente_cpf,
        ARRAY_AGG(STRUCT(
            COALESCE(sm.cns, vc.cns) AS valor,
            COALESCE(sm.rank, vc.rank) AS rank
        )) AS cns
    FROM vitacare_cns_ranked  vc
    FULL OUTER JOIN smsrio_cns_ranked  sm
        ON vc.paciente_cpf = sm.paciente_cpf
    GROUP BY sm.paciente_cpf, vc.paciente_cpf
),

-- Clinica Familia Dados: Groups family clinic data by patient.
clinica_familia_dados AS (
    SELECT
        paciente_cpf AS paciente_cpf,
        ARRAY_AGG(STRUCT(
            id_cnes, 
            nome, 
            datahora_ultima_atualizacao, 
            rank
        )) AS clinica_familia
    FROM vitacare_clinica_familia
    GROUP BY paciente_cpf
),

-- Equipe Saude Familia Dados: Groups family health team data by patient.
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

-- Contato Dados: Merges contact data
contato_dados AS (
    SELECT
        COALESCE(
            COALESCE(vt_telefone.paciente_cpf, vt_email.paciente_cpf), 
            COALESCE(sm_telefone.paciente_cpf, sm_email.paciente_cpf)
        ) AS paciente_cpf,
        STRUCT(
            ARRAY_AGG(STRUCT(
                COALESCE(vt_telefone.valor, sm_telefone.valor) AS valor, 
                COALESCE(vt_telefone.rank, sm_telefone.rank) AS rank
            )) AS telefone,
            ARRAY_AGG(STRUCT(
                COALESCE(vt_email.valor, sm_email.valor) AS valor, 
                COALESCE(vt_email.rank, sm_email.rank) AS rank
            )) AS email
        ) AS contato
    FROM vitacare_contato_telefone vt_telefone
    FULL OUTER JOIN vitacare_contato_email vt_email
        ON vt_telefone.paciente_cpf = vt_email.paciente_cpf
    FULL OUTER JOIN smsrio_contato_telefone sm_telefone
        ON vt_telefone.paciente_cpf = sm_telefone.paciente_cpf
    FULL OUTER JOIN smsrio_contato_email sm_email
        ON vt_telefone.paciente_cpf = sm_email.paciente_cpf
    GROUP BY vt_telefone.paciente_cpf, vt_email.paciente_cpf, sm_telefone.paciente_cpf, sm_email.paciente_cpf
),

-- Endereco Dados: Merges address information
endereco_dados AS (
    SELECT
        COALESCE(vc.paciente_cpf,sm.paciente_cpf) AS paciente_cpf,
        ARRAY_AGG(STRUCT(
            COALESCE(vc.cep,sm.cep) AS cep, 
            COALESCE(vc.tipo_logradouro,sm.tipo_logradouro) AS tipo_logradouro, 
            COALESCE(vc.logradouro,sm.logradouro) AS logradouro, 
            COALESCE(vc.numero,sm.numero) AS numero, 
            COALESCE(vc.complemento,sm.complemento) AS complemento, 
            COALESCE(vc.bairro,sm.bairro) AS bairro, 
            COALESCE(vc.cidade,sm.cidade) AS cidade, 
            COALESCE(vc.estado,sm.estado) AS estado, 
            COALESCE(vc.datahora_ultima_atualizacao,sm.datahora_ultima_atualizacao) AS datahora_ultima_atualizacao, 
            COALESCE(vc.rank,sm.rank) AS rank
        )) AS endereco
    FROM vitacare_endereco  vc
    FULL OUTER JOIN smsrio_endereco  sm
        ON vc.paciente_cpf = sm.paciente_cpf
    GROUP BY sm.paciente_cpf, vc.paciente_cpf

),

-- Prontuario Dados: Merges system medical record data
prontuario_dados AS (
    SELECT
        COALESCE(vp.paciente_cpf,sp.paciente_cpf) AS paciente_cpf,
        STRUCT(ARRAY_AGG(STRUCT(
            vp.sistema, 
            vp.id_cnes, 
            vp.id_paciente, 
            vp.rank
        )) AS vitacare,
        ARRAY_AGG(STRUCT(
            sp.sistema, 
            sp.id_cnes, 
            sp.id_paciente, 
            sp.rank
        )) AS smsrio
        ) AS prontuario
    FROM vitacare_prontuario vp
    FULL OUTER JOIN smsrio_prontuario sp
        ON vp.paciente_cpf = sp.paciente_cpf
    GROUP BY COALESCE(vp.paciente_cpf,sp.paciente_cpf)
),

-- Paciente Dados: Merges patient data
paciente_dados AS (
    SELECT
        COALESCE(vc.paciente_cpf, sm.paciente_cpf) AS paciente_cpf,
        ARRAY_AGG(STRUCT(
            COALESCE(vc.nome, sm.nome) AS nome,
            COALESCE(vc.nome_social, sm.nome_social) AS nome_social_social,
            COALESCE(vc.cpf, sm.cpf) AS cpf,
            COALESCE(vc.data_nascimento, sm.data_nascimento) AS data_nascimento,
            COALESCE(vc.genero, sm.genero) AS genero,
            COALESCE(vc.raca, sm.raca) AS raca,
            COALESCE(vc.obito_indicador,sm.obito_indicador) AS obito_indicador,
            COALESCE(sm.obito_data, vc.obito_data) AS obito_data,
            COALESCE(vc.mae_nome, sm.mae_nome) AS mae_nome,
            COALESCE(vc.pai_nome, sm.pai_nome) AS pai_nome,
            COALESCE(vc.rank, sm.rank) AS rank
        )) AS dados
    FROM vitacare_paciente vc
    FULL OUTER JOIN smsrio_paciente sm
        ON vc.paciente_cpf = sm.paciente_cpf
    GROUP BY vc.paciente_cpf, sm.paciente_cpf
),

---- FINAL JOIN: Joins all the data previously processed, creating the
---- integrated table of the patients.

paciente_integrado AS (
    SELECT
        pd.paciente_cpf,
        cns.cns,
        pd.dados,
        cf.clinica_familia,
        esf.equipe_saude_familia,
        ct.contato,
        ed.endereco,
        pt.prontuario,
    FROM paciente_dados pd
    LEFT JOIN cns_dados cns ON pd.paciente_cpf = cns.paciente_cpf
    LEFT JOIN clinica_familia_dados cf ON pd.paciente_cpf = cf.paciente_cpf
    LEFT JOIN equipe_saude_familia_dados esf ON pd.paciente_cpf = esf.paciente_cpf
    LEFT JOIN contato_dados ct ON pd.paciente_cpf = ct.paciente_cpf
    LEFT JOIN endereco_dados ed ON pd.paciente_cpf = ed.paciente_cpf
    LEFT JOIN prontuario_dados pt ON pd.paciente_cpf = pt.paciente_cpf
)


SELECT * 
FROM paciente_integrado
WHERE EXISTS (SELECT 1 FROM UNNEST(prontuario.smsrio) WHERE id_paciente IS NOT NULL)
    AND EXISTS (SELECT 1 FROM UNNEST(prontuario.vitacare) WHERE id_paciente IS NOT NULL)