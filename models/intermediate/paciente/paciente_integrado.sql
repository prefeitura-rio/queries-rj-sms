-- This code integrates patient data from two sources:
-- rj-sms.brutos_prontuario_vitacare.paciente (VITACARE)
-- rj-sms.brutos_plataforma_vitai.paciente (VITAI)
-- rj-sms.brutos_plataforma_smsrio.paciente (SMSRIO)
-- The goal is to consolidate information such as registration data,
-- contact, address and medical record into a single view.

-- Declaration of the variable to filter by CPF (optional)
DECLARE cpf_filter STRING DEFAULT "";

-- Auxiliary function to clean and standardize text fields
CREATE TEMP FUNCTION CleanText(texto STRING) AS (
    TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(texto, NFD), r'\pM', '')))
);

---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
--  Get source data and standardize
---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--

-- VITACARE: Patient base table
WITH vitacare_tb AS (
SELECT
        CleanText(cpf) AS paciente_cpf,
        CleanText(cns) AS cns,
        CleanText(cnes_unidade) AS cnes_unidade,
        CleanText(codigo_ine_equipe_saude) AS codigo_ine_equipe_saude,
        CleanText(telefone) AS telefone,
        CleanText(email) AS email,
        CleanText(endereco_cep) AS endereco_cep,
        CleanText(endereco_tipo_logradouro) AS endereco_tipo_logradouro,
        CleanText(endereco_logradouro) AS logradouro,
        CleanText(endereco_bairro) AS bairro,
        CleanText(endereco_municipio) AS municipio_residencia,
        CleanText(endereco_estado) AS estado_residencia,
        CleanText(id) AS id,
        CleanText(cpf) AS cpf,
        CleanText(nome) AS nome,
        CleanText(nome_social) AS nome_social,
        CleanText(sexo) AS sexo,
        CleanText(raca_cor) AS raca_cor,
        CleanText(nome_mae) AS nome_mae,
        CleanText(nome_pai) AS nome_pai,
        cadastro_permanente,
        data_obito,
        data_nascimento,
        updated_at AS data_atualizacao_vinculo_equipe,
        updated_at
    FROM `rj-sms.brutos_prontuario_vitacare.paciente`
    WHERE cpf IS NOT NULL
        AND NOT REGEXP_CONTAINS(cpf, r'[A-Za-z]')
        AND TRIM(cpf) != ""
        AND tipo = "rotineiro"

        -- AND cpf = cpf_filter
),

-- SMSRIO: Patient base table
smsrio_tb AS (
    SELECT 
        CleanText(cpf) AS paciente_cpf,
        CleanText(cns_lista) AS cns_provisorio,
        CleanText(telefone_lista) AS telefones,
        CleanText(email) AS email,
        CleanText(endereco_logradouro) AS endereco_logradouro,
        CleanText(endereco_cep) AS endereco_cep,
        CleanText(endereco_tipo_logradouro) AS endereco_tipo_logradouro,
        CleanText(endereco_numero) AS endereco_numero,
        CleanText(endereco_complemento) AS endereco_complemento,
        CleanText(endereco_bairro) AS endereco_bairro,
        CleanText(endereco_municipio_codigo) AS endereco_municipio_codigo,
        CleanText(endereco_uf) AS endereco_uf,
        CleanText(sexo) AS sexo,
        CleanText(raca_cor) AS raca_cor,
        CleanText(obito) AS obito,
        CleanText(nome_mae) AS nome_mae,
        CleanText(nome_pai) AS nome_pai,
        CleanText(nome) AS nome,
        data_nascimento,
        data_obito,
        updated_at
    FROM `rj-sms.brutos_plataforma_smsrio.paciente`
    WHERE cpf IS NOT NULL
        AND NOT REGEXP_CONTAINS(cpf, r'[A-Za-z]')
        AND TRIM(cpf) != ""
--     WHERE cpf = cpf_filter
),

vitai_tb AS (
    SELECT 
        CleanText(cpf) AS cpf,
        CleanText(cns) AS cns,
        CleanText(cliente) AS cliente,
        CleanText(nome) AS nome,
        CleanText(telefone) AS telefone,
        CleanText(tipo_logradouro) AS endereco_tipo_logradouro,
        CleanText(nome_logradouro) AS nome_logradouro,
        CleanText(numero) AS numero,
        CleanText(complemento) AS complemento,
        CleanText(bairro) AS bairro,
        CleanText(municipio) AS municipio,
        CleanText(uf) AS uf,
        CleanText(id_cidadao) AS id_cidadao,
        CleanText(nome_alternativo) AS nome_alternativo,
        CleanText(sexo) AS sexo,
        CleanText(raca_cor) AS raca_cor,
        CleanText(nome_mae) AS nome_mae,
        gid_estabelecimento,
        data_obito,
        data_nascimento,
        updated_at
    FROM `rj-sms.brutos_prontuario_vitai.paciente`
    WHERE cpf IS NOT NULL
        AND NOT REGEXP_CONTAINS(cpf, r'[A-Za-z]')
        AND TRIM(cpf) != ""
        -- AND cpf = cpf_filter
),
--

---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
--  Get CNS from the source tables
-- giving preference to the most recently registered.
---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--

-- CNS VITACARE: Extracts and ranks CNS numbers
vitacare_cns_ranked AS (
    SELECT
        paciente_cpf,
        cns,
        ROW_NUMBER() OVER (PARTITION BY paciente_cpf, cns ORDER BY data_atualizacao_vinculo_equipe DESC, cadastro_permanente DESC, updated_at DESC) AS original_rank,
    FROM vitacare_tb
    WHERE
        cns IS NOT NULL
    GROUP BY paciente_cpf, cns, data_atualizacao_vinculo_equipe, cadastro_permanente, updated_at
),

-- CNS SMSRIO: Extracts and ranks CNS numbers 
smsrio_cns_ranked AS (
    SELECT
        paciente_cpf,
        CASE 
            WHEN TRIM(cns) IN ('NONE') THEN NULL
            ELSE TRIM(cns)
        END AS cns,
        ROW_NUMBER() OVER (PARTITION BY paciente_cpf ORDER BY updated_at DESC) AS original_rank,
    FROM (
            SELECT
                paciente_cpf,
                cns,
                updated_at
            FROM smsrio_tb,
            UNNEST(SPLIT(REPLACE(REPLACE(REPLACE(cns_provisorio, '[', ''), ']', ''), '"', ''), ',')) AS cns
    )
    GROUP BY paciente_cpf, cns, updated_at
    
),

-- CNS VITAI: Extracts and ranks CNS numbers 
vitai_cns_ranked AS (
    SELECT
        cpf AS paciente_cpf,
        cns,
        ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY updated_at DESC) AS rank
    FROM vitai_tb
    WHERE
        cns IS NOT NULL
        AND TRIM(cns) NOT IN ("")
    GROUP BY cpf, cns, updated_at
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
        e.nome_limpo AS nome,
        data_atualizacao_vinculo_equipe AS datahora_ultima_atualizacao,
        ROW_NUMBER() OVER (PARTITION BY paciente_cpf ORDER BY data_atualizacao_vinculo_equipe DESC, cadastro_permanente DESC, updated_at DESC) AS original_rank
    FROM vitacare_tb vc
    JOIN `rj-sms.saude_dados_mestres.estabelecimento` e
        ON vc.cnes_unidade = e.id_cnes
    GROUP BY
        paciente_cpf, cnes_unidade, e.nome_limpo, data_atualizacao_vinculo_equipe, cadastro_permanente, updated_at
),

---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
--  Get Family Health Team data
--  attended by each patient based on the most recent visit date.
---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--

-- EQUIPE SAUDE FAMILIA VITACARE: Extracts and ranks family health teams
vitacare_equipe_saude_familia AS (
    SELECT
        paciente_cpf,
        codigo_ine_equipe_saude AS id_ine,
        data_atualizacao_vinculo_equipe AS datahora_ultima_atualizacao,
        ROW_NUMBER() OVER (PARTITION BY paciente_cpf ORDER BY data_atualizacao_vinculo_equipe DESC, cadastro_permanente DESC, updated_at DESC) AS original_rank
    FROM vitacare_tb
    WHERE
        codigo_ine_equipe_saude IS NOT NULL
    GROUP BY
        paciente_cpf, codigo_ine_equipe_saude, data_atualizacao_vinculo_equipe, cadastro_permanente, updated_at
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
        original_rank
    FROM (
        SELECT
            paciente_cpf,
            'telefone' AS tipo,
            telefone AS valor,
            ROW_NUMBER() OVER (PARTITION BY paciente_cpf ORDER BY data_atualizacao_vinculo_equipe DESC, cadastro_permanente DESC, updated_at DESC) AS original_rank
        FROM vitacare_tb
        GROUP BY paciente_cpf, telefone, data_atualizacao_vinculo_equipe, cadastro_permanente, updated_at
    )
    WHERE NOT (TRIM(valor) IN ("()", "") AND (original_rank >= 2))
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
        original_rank
    FROM (
        SELECT
            paciente_cpf,
            'email' AS tipo,
            email AS valor,
            ROW_NUMBER() OVER (PARTITION BY paciente_cpf ORDER BY data_atualizacao_vinculo_equipe DESC, cadastro_permanente DESC, updated_at DESC) AS original_rank
        FROM vitacare_tb
        GROUP BY paciente_cpf, email, data_atualizacao_vinculo_equipe, cadastro_permanente, updated_at
    )
    WHERE NOT (TRIM(valor) IN ("()", "") AND (original_rank >= 2))
),

vitacare_contato_array AS (
    SELECT
        COALESCE(vt_telefone.paciente_cpf, vt_email.paciente_cpf) AS paciente_cpf, 
        STRUCT(
            ARRAY_AGG(STRUCT(
                vt_telefone.valor AS valor, 
                vt_telefone.original_rank AS original_rank,
                "VITACARE" AS sistema
            )) AS telefone,
            ARRAY_AGG(STRUCT(
                vt_email.valor  AS valor, 
                vt_email.original_rank AS original_rank,
                "VITACARE" AS sistema
            )) AS email
        ) AS vitacare,
    FROM vitacare_contato_telefone vt_telefone
    FULL OUTER JOIN vitacare_contato_email vt_email
        ON vt_telefone.paciente_cpf = vt_email.paciente_cpf
    GROUP BY vt_telefone.paciente_cpf, vt_email.paciente_cpf
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
        original_rank
    FROM (
        SELECT
            paciente_cpf,
            'telefone' AS tipo,
            TRIM(telefones) AS valor,
            ROW_NUMBER() OVER (PARTITION BY paciente_cpf ORDER BY updated_at DESC) AS original_rank
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
    WHERE NOT (TRIM(valor) IN ("NONE", "NULL", "") AND (original_rank >= 2))
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
        original_rank
    FROM (
        SELECT
            paciente_cpf,
            'email' AS tipo,
            email AS valor,
            ROW_NUMBER() OVER (PARTITION BY paciente_cpf ORDER BY updated_at DESC) AS original_rank
        FROM smsrio_tb
        GROUP BY
            paciente_cpf, email, updated_at
    )
    WHERE NOT (TRIM(valor) IN ("NONE", "NULL", "") AND (original_rank >= 2))
),


smsrio_contato_array AS (
    SELECT
        COALESCE(sm_telefone.paciente_cpf, sm_email.paciente_cpf) AS paciente_cpf, 
        STRUCT(
            ARRAY_AGG(STRUCT(
                sm_telefone.valor AS valor, 
                sm_telefone.original_rank AS original_rank,
                "SMSRIO" AS sistema
            )) AS telefone,
            ARRAY_AGG(STRUCT(
                sm_email.valor  AS valor, 
                sm_email.original_rank AS original_rank,
                "SMSRIO" AS sistema
            )) AS email
        ) AS smsrio,
    FROM smsrio_contato_telefone sm_telefone
    FULL OUTER JOIN smsrio_contato_email sm_email
        ON sm_telefone.paciente_cpf = sm_email.paciente_cpf
    GROUP BY sm_telefone.paciente_cpf, sm_email.paciente_cpf
),


-- CONTATO VIRAI: Extracts and ranks phone numbers
vitai_contato_telefone AS (
    SELECT 
        paciente_cpf,
        tipo, 
        CASE 
            WHEN TRIM(valor) IN ("()", "") THEN NULL
            ELSE valor
        END AS valor,
        original_rank
    FROM (
        SELECT
            cpf AS paciente_cpf,
            'telefone' AS tipo,
            telefone AS valor,
            ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY updated_at DESC) AS original_rank
        FROM vitai_tb
        GROUP BY cpf, telefone, updated_at
    )
    WHERE NOT (TRIM(valor) IN ("()", "") AND (original_rank >= 2))
),

-- CONTATO VIRAI: Extracts and ranks email
vitai_contato_email AS (
    SELECT 
        paciente_cpf,
        tipo, 
        CASE 
            WHEN TRIM(valor) IN ("()", "") THEN NULL
            ELSE valor
        END AS valor,
        original_rank
    FROM (
        SELECT
            cpf AS paciente_cpf,
            'email' AS tipo,
            "" AS valor, 
            ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY updated_at DESC) AS original_rank
        FROM vitai_tb
        GROUP BY cpf, updated_at
    )
    WHERE NOT (TRIM(valor) IN ("()", "") AND (original_rank >= 2))
),

vitai_contato_array AS (
    SELECT
        COALESCE(vi_telefone.paciente_cpf, vi_email.paciente_cpf) AS paciente_cpf, 
        STRUCT(
            ARRAY_AGG(STRUCT(
                vi_telefone.valor AS valor, 
                vi_telefone.original_rank AS original_rank,
                "VITAI" AS sistema
            )) AS telefone,
            ARRAY_AGG(STRUCT(
                vi_email.valor  AS valor, 
                vi_email.original_rank AS original_rank,
                "VITAI" AS sistema
            )) AS email
        ) AS vitai,
    FROM vitai_contato_telefone vi_telefone
    FULL OUTER JOIN vitai_contato_email vi_email
        ON vi_telefone.paciente_cpf = vi_email.paciente_cpf
    GROUP BY vi_telefone.paciente_cpf, vi_email.paciente_cpf
),

---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
--  Get address information
--  prioritizing the most recent by registration date.
---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--

--  ENDEREÇO VITACARE: Extracts and ranks addresses, 
vitacare_endereco AS (
    SELECT
        paciente_cpf,
        endereco_cep AS cep,
        endereco_tipo_logradouro AS tipo_logradouro,
        REGEXP_EXTRACT(logradouro, r'^(.*?)(?:\d+.*)?$') AS logradouro,
        REGEXP_EXTRACT(logradouro, r'\b(\d+)\b') AS numero,
        TRIM(REGEXP_REPLACE(logradouro, r'^.*?\d+\s*(.*)$', r'\1')) AS complemento,
        bairro,
        municipio_residencia AS cidade,
        estado_residencia AS estado,
        CAST(cadastro_permanente AS STRING) AS datahora_ultima_atualizacao,
        ROW_NUMBER() OVER (PARTITION BY paciente_cpf ORDER BY data_atualizacao_vinculo_equipe, cadastro_permanente, updated_at DESC) AS original_rank
    FROM vitacare_tb
    WHERE
        logradouro IS NOT NULL
    GROUP BY
        paciente_cpf, endereco_cep, tipo_logradouro, logradouro,REGEXP_EXTRACT(logradouro, r'\b(\d+)\b'),TRIM(REGEXP_REPLACE(logradouro, r'^.*?\d+\s*(.*)$', r'\1')), bairro, municipio_residencia, estado_residencia, data_atualizacao_vinculo_equipe, cadastro_permanente, updated_at
),

vitacare_endereco_array AS (
    SELECT
        vc.paciente_cpf AS paciente_cpf,
        ARRAY_AGG(STRUCT(
            vc.cep AS cep, 
            vc.tipo_logradouro AS tipo_logradouro, 
            vc.logradouro AS logradouro, 
            vc.numero AS numero, 
            vc.complemento AS complemento, 
            vc.bairro AS bairro, 
            vc.cidade AS cidade, 
            vc.estado AS estado, 
            vc.datahora_ultima_atualizacao AS datahora_ultima_atualizacao,
            vc.original_rank AS original_rank,
            "VITACARE" AS sistema
        )) AS vitacare
    FROM vitacare_endereco  vc
    GROUP BY vc.paciente_cpf
),

--  ENDEREÇO SMSRIO: Extracts and ranks addresses
smsrio_endereco AS (
    SELECT
        paciente_cpf,
        endereco_cep AS cep,
        CASE 
            WHEN endereco_tipo_logradouro IN ("NONE","") THEN NULL
            ELSE endereco_tipo_logradouro
        END AS tipo_logradouro,
        endereco_logradouro AS logradouro,
        endereco_numero AS numero,
        endereco_complemento AS complemento,
        endereco_bairro AS bairro,
        CASE 
            WHEN endereco_municipio_codigo IN ("NONE","") THEN NULL
            ELSE endereco_municipio_codigo
        END AS cidade,
        endereco_uf AS estado,
        CAST(updated_at AS STRING) AS datahora_ultima_atualizacao,
        ROW_NUMBER() OVER (PARTITION BY paciente_cpf ORDER BY updated_at DESC) AS original_rank
    FROM smsrio_tb
    GROUP BY
        paciente_cpf, endereco_cep, endereco_tipo_logradouro, endereco_logradouro, endereco_numero, endereco_complemento, endereco_bairro, endereco_municipio_codigo, endereco_uf, updated_at
),

smsrio_endereco_array AS (
    SELECT
        sm.paciente_cpf AS paciente_cpf,
        ARRAY_AGG(STRUCT(
                sm.cep AS cep, 
                sm.tipo_logradouro AS tipo_logradouro, 
                sm.logradouro AS logradouro, 
                sm.numero AS numero, 
                sm.complemento AS complemento, 
                sm.bairro AS bairro, 
                sm.cidade AS cidade, 
                sm.estado AS estado, 
                sm.datahora_ultima_atualizacao AS datahora_ultima_atualizacao,
                sm.original_rank AS original_rank,
                "SMSRIO" AS sistema
        )) AS smsrio
    FROM smsrio_endereco sm
    GROUP BY sm.paciente_cpf
),

--  ENDEREÇO VITAI: Extracts and ranks addresses
vitai_endereco AS (
    SELECT
        cpf AS paciente_cpf,
        CAST(NULL AS STRING) AS cep,
        endereco_tipo_logradouro AS tipo_logradouro,
        CASE
            WHEN nome_logradouro in ("NONE") THEN NULL
            ELSE nome_logradouro
        END AS logradouro,
        numero AS numero,
        complemento AS complemento,
        bairro,
        municipio AS cidade,
        uf AS estado,
        CAST(updated_at AS STRING) AS datahora_ultima_atualizacao,
        ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY updated_at DESC) AS original_rank
    FROM vitai_tb
    WHERE
        nome_logradouro IS NOT NULL
    GROUP BY
        cpf, tipo_logradouro, nome_logradouro, numero, complemento, bairro, municipio, uf, updated_at
),

vitai_endereco_array AS (
    SELECT
        vi.paciente_cpf AS paciente_cpf,
        ARRAY_AGG(STRUCT(
            vi.cep AS cep, 
            vi.tipo_logradouro AS tipo_logradouro, 
            vi.logradouro AS logradouro, 
            vi.numero AS numero, 
            vi.complemento AS complemento, 
            vi.bairro AS bairro, 
            vi.cidade AS cidade, 
            vi.estado AS estado, 
            vi.datahora_ultima_atualizacao AS datahora_ultima_atualizacao,
            vi.original_rank AS original_rank,
            "VITAI" AS sistema
        )) AS vitai
    FROM vitai_endereco  vi
    GROUP BY vi.paciente_cpf
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
        ROW_NUMBER() OVER (PARTITION BY paciente_cpf ORDER BY data_atualizacao_vinculo_equipe DESC, cadastro_permanente DESC, updated_at DESC) AS original_rank
    FROM vitacare_tb
    GROUP BY
        paciente_cpf, cnes_unidade, id, data_atualizacao_vinculo_equipe, cadastro_permanente, updated_at
),

vitacare_prontuario_array AS (
    SELECT
        vc.paciente_cpf,
        ARRAY_AGG(STRUCT(
            vc.sistema, 
            vc.id_cnes, 
            vc.id_paciente, 
            vc.original_rank
        )) AS vitacare
    FROM vitacare_prontuario vc
    GROUP BY vc.paciente_cpf
),

-- PRONTUARIO SMSRIO: Extracts and ranks medical record information
smsrio_prontuario AS (
    SELECT
        paciente_cpf,
        'SMSRIO' AS sistema,
        NULL AS id_cnes,
        paciente_cpf AS id_paciente,
        ROW_NUMBER() OVER (PARTITION BY paciente_cpf ORDER BY updated_at DESC) AS original_rank
    FROM smsrio_tb
    GROUP BY
        paciente_cpf, endereco_municipio_codigo, updated_at
),

smsrio_prontuario_array AS (
    SELECT
        sm.paciente_cpf,
        ARRAY_AGG(STRUCT(
            sm.sistema, 
            sm.id_cnes, 
            sm.id_paciente, 
            sm.original_rank
        )) AS smsrio
    FROM smsrio_prontuario sm
    GROUP BY sm.paciente_cpf
),

-- PRONTUARIO VITAI: Extracts and ranks medical record information
vitai_prontuario AS (
    SELECT
        cpf AS paciente_cpf,
        'VITAI' AS sistema,
        id_cnes AS id_cnes,
        cpf AS id_paciente,
        ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY updated_at DESC) AS original_rank
    FROM(
        SELECT 
            pc.updated_at,
            pc.cpf,
            es.cnes AS id_cnes,
        FROM  vitai_tb pc
        JOIN  `rj-sms.brutos_prontuario_vitai.estabelecimento` es
            ON pc.gid_estabelecimento = es.gid
    )
    GROUP BY
        cpf, id_cnes, updated_at
),

vitai_prontuario_array AS (
    SELECT
        vi.paciente_cpf,
        ARRAY_AGG(STRUCT(
            vi.sistema, 
            vi.id_cnes, 
            vi.id_paciente, 
            vi.original_rank
        )) AS vitai
    FROM vitai_prontuario vi
    GROUP BY vi.paciente_cpf
),

---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
--  Get and structure patient data
---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--

-- PACIENTE DADOS VITACARE: Extracts and structures patient data
vitacare_paciente AS (
    SELECT
        paciente_cpf,
        nome,
        CASE 
            WHEN nome_social IN ('') THEN NULL
            ELSE nome_social
        END AS nome_social,
        cpf,
        DATE(data_nascimento) AS data_nascimento,
        sexo AS genero,
        CASE
            WHEN TRIM(raca_cor) IN ("") THEN NULL
            ELSE raca_cor
        END AS raca,
        CASE
            WHEN data_obito IS NULL THEN FALSE
            WHEN data_obito IS NOT NULL THEN TRUE
            ELSE NULL
        END AS obito_indicador,
        data_obito AS obito_data,
        nome_mae AS mae_nome,
        nome_pai AS pai_nome,
        ROW_NUMBER() OVER (PARTITION BY paciente_cpf ORDER BY updated_at) AS original_rank
    FROM vitacare_tb
    GROUP BY paciente_cpf, nome, nome_social, cpf, data_nascimento, sexo, raca_cor, data_obito, nome_mae, nome_pai,updated_at
),

-- PACIENTE DADOS SMSRIO: Extracts and structures patient data
smsrio_paciente AS (
    SELECT
        paciente_cpf,
        nome,
        CAST(NULL AS STRING) AS nome_social,
        paciente_cpf AS cpf,
        DATE(data_nascimento) AS data_nascimento,
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
        DATE(data_obito) AS obito_data,
        nome_mae AS mae_nome,
        nome_pai AS pai_nome,
        ROW_NUMBER() OVER (PARTITION BY paciente_cpf ORDER BY updated_at) AS original_rank
    FROM smsrio_tb
    GROUP BY
        paciente_cpf, nome, DATE(data_nascimento), sexo, raca_cor, obito, data_obito, nome_mae, nome_pai, updated_at
),

-- PACIENTE DADOS VITAI: Extracts and structures patient data
vitai_paciente AS (
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
        CAST(NULL AS STRING) AS pai_nome,
        FALSE AS cadastro_validado_indicador,
        ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY updated_at) AS original_rank
    FROM vitai_tb
    GROUP BY
        cpf, nome, nome_alternativo, cpf, DATE(data_nascimento), sexo, raca_cor, data_obito, nome_mae, updated_at
),

---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
--  Merge data from different sources
---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--

-- CNS Dados: Merges CNS data, grouping by patient 
-- UNION 1. Vitacare | 3. SMSRIO | 2. Vitai
cns_dedup AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY paciente_cpf  ORDER BY merge_order ASC, original_rank ASC) AS rank
    FROM(
        SELECT 
            *,
            ROW_NUMBER() OVER (PARTITION BY paciente_cpf, cns ORDER BY merge_order, original_rank ASC) AS dedup_rank,
        FROM (
            SELECT 
                *,
                "VITACARE" AS sistema,
                1 AS merge_order
            FROM vitacare_cns_ranked
            UNION ALL 
            SELECT 
                *,
                "VITAI" AS sistema,
                2 AS merge_order
            FROM vitai_cns_ranked
            UNION ALL 
            SELECT 
                *,
                "SMSRIO" AS sistema,
                3 AS merge_order
            FROM smsrio_cns_ranked
        )
        ORDER BY  merge_order ASC, original_rank ASC 
    )
    WHERE dedup_rank = 1
    ORDER BY  merge_order ASC, original_rank ASC 
),


cns_dados AS (
    SELECT 
        paciente_cpf,
        ARRAY_AGG(
                STRUCT(
                    cns, 
                    rank,
                    sistema
                )
        ) AS cns
    FROM cns_dedup
    GROUP BY paciente_cpf
),



-- Clinica Familia Dados: Groups family clinic data by patient.
-- ONLY VITACARE
clinica_familia_dados AS (
    SELECT
        paciente_cpf AS paciente_cpf,
        ARRAY_AGG(STRUCT(
            id_cnes, 
            nome, 
            datahora_ultima_atualizacao, 
            original_rank
        )) AS clinica_familia
    FROM vitacare_clinica_familia
    GROUP BY paciente_cpf
),

-- Equipe Saude Familia Dados: Groups family health team data by patient.
-- ONLY VITACARE
equipe_saude_familia_dados AS (
    SELECT
        paciente_cpf,
        ARRAY_AGG(STRUCT(
            id_ine, 
            datahora_ultima_atualizacao, 
            original_rank
        )) AS equipe_saude_familia
    FROM vitacare_equipe_saude_familia
    GROUP BY paciente_cpf
),

-- Contato Dados: Merges contact data 
-- UNION: 1. Vitacare | 2. SMSRIO | 3. Vitai
contato_dados AS (
    SELECT
        COALESCE(
            vc.paciente_cpf,
            sm.paciente_cpf,
            vi.paciente_cpf
        ) AS paciente_cpf,
        STRUCT(
            ARRAY_CONCAT(vc.vitacare.telefone, sm.smsrio.telefone, vi.vitai.telefone) AS telefone,
            ARRAY_CONCAT(vc.vitacare.email, sm.smsrio.email, vi.vitai.email) AS email
        ) AS contato
    FROM vitacare_contato_array vc
    FULL OUTER JOIN smsrio_contato_array sm
        ON vc.paciente_cpf = sm.paciente_cpf
    FULL OUTER JOIN vitai_contato_array vi
        ON vc.paciente_cpf = vi.paciente_cpf
),

-- Endereco Dados: Merges address information
-- UNION: 1. Vitacare | 2. SMSRIO | 3. Vitai
endereco_dados AS (
    SELECT
        COALESCE(
            vc.paciente_cpf,
            sm.paciente_cpf,
            vi.paciente_cpf
        ) AS paciente_cpf,
        ARRAY_CONCAT(vc.vitacare, sm.smsrio) AS endereco
    FROM vitacare_endereco_array vc
    FULL OUTER JOIN smsrio_endereco_array sm
        ON vc.paciente_cpf = sm.paciente_cpf
    FULL OUTER JOIN vitai_endereco_array vi
        ON vc.paciente_cpf = vi.paciente_cpf
),

-- Prontuario Dados: Merges system medical record data
-- UNION: 1. Vitacare | 2. SMSRIO | 3. Vitai
prontuario_dados AS (
    SELECT
        COALESCE(
            vc.paciente_cpf,
            sm.paciente_cpf,
            vi.paciente_cpf
        ) AS paciente_cpf,
        STRUCT(
            vc.vitacare,
            sm.smsrio,
            vi.vitai
        ) AS prontuario
    FROM vitacare_prontuario_array vc
    FULL OUTER JOIN smsrio_prontuario_array sm
        ON vc.paciente_cpf = sm.paciente_cpf
    FULL OUTER JOIN vitai_prontuario_array vi
        ON vc.paciente_cpf = vi.paciente_cpf
),

-- Paciente Dados: Merges patient data
-- COALESCE
-- nome:             1. SMSRIO   | 2. Vitai    | 3. Vitacare
-- nome_social:      1. SMSRIO   | 2. Vitai    | 3. Vitacare
-- cpf:              1. SMSRIO   | 2. Vitai    | 3. Vitacare
-- data_nascimento:  1. SMSRIO   | 2. Vitacare | 3. Vitai
-- genero:           1. SMSRIO   | 2. Vitai    | 3. Vitacare
-- raca:             1. Vitacare | 2. SMSRIO   | 3. Vitai
-- obito_indicador:  1. SMSRIO   | 2. Vitai    | 3. Vitacare
-- obito_data:       1. SMSRIO   | 2. Vitai    | 3. Vitacare
-- mae_nome:         1. SMSRIO   | 2. Vitai    | 3. Vitacare
-- pai_nome:         1. SMSRIO   | 2. Vitai    | 3. Vitacare

paciente_dados AS (
    SELECT
        COALESCE(sm.paciente_cpf, vi.paciente_cpf, vc.paciente_cpf) AS paciente_cpf,
        STRUCT(
                COALESCE(sm.nome, vi.nome, vc.nome) AS nome,
                COALESCE(sm.nome_social, vi.nome_social, vc.nome_social) AS nome_social,
                COALESCE(sm.cpf, vi.cpf, vc.cpf) AS cpf,
                COALESCE(sm.data_nascimento, vi.data_nascimento, vc.data_nascimento) AS  data_nascimento,
                COALESCE(sm.genero, vi.genero, vc.genero) AS genero,
                COALESCE(vc.raca, sm.raca, vi.raca) AS raca,
                COALESCE(sm.obito_indicador, vi.obito_indicador, vc.obito_indicador) AS obito_indicador,
                COALESCE(sm.obito_data, vi.obito_data, vc.obito_data) AS obito_data,
                COALESCE(sm.mae_nome, vi.mae_nome, vc.mae_nome) AS mae_nome,
                COALESCE(sm.pai_nome, vi.pai_nome, vc.pai_nome) AS pai_nome,
                CASE 
                    WHEN sm.nome IS NOT NULL AND sm.data_nascimento IS NOT NULL AND sm.mae_nome IS NOT NULL THEN TRUE
                    ELSE FALSE 
                END AS cadastro_validado_indicador
                -- COALESCE(sm.original_rank, vc.original_rank) AS original_rank
        ) AS dados
    FROM vitacare_paciente vc
    FULL OUTER JOIN smsrio_paciente sm
        ON vc.paciente_cpf = sm.paciente_cpf
    FULL OUTER JOIN vitai_paciente vi
        ON vc.paciente_cpf = vi.paciente_cpf
    GROUP BY sm.paciente_cpf, vi.paciente_cpf, vc.paciente_cpf, 
        sm.nome, vi.nome, vc.nome,
        sm.nome_social, vi.nome_social, vc.nome_social,
        sm.cpf, vi.cpf, vc.cpf, 
        sm.data_nascimento, vi.data_nascimento, vc.data_nascimento,
        sm.genero, vi.genero, vc.genero,
        vc.raca, sm.raca, vi.raca,
        sm.obito_indicador, vi.obito_indicador, vc.obito_indicador, 
        sm.obito_data, vi.obito_data, vc.obito_data,
        sm.mae_nome, vi.mae_nome, vc.mae_nome, 
        sm.pai_nome, vi.pai_nome, vc.pai_nome,
        CASE 
            WHEN sm.nome IS NOT NULL AND sm.data_nascimento IS NOT NULL AND sm.mae_nome IS NOT NULL THEN TRUE
            ELSE FALSE 
        END 
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
        STRUCT(CURRENT_TIMESTAMP() AS created_at) AS metadados
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
    AND EXISTS (SELECT 1 FROM UNNEST(prontuario.vitai) WHERE id_paciente IS NOT NULL)
