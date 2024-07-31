DECLARE cpf_filter STRING DEFAULT "";

WITH vitacare_tb AS (
    SELECT 
        TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(paciente_cpf, NFD), r'\pM', ''))) AS paciente_cpf,
        TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(cns, NFD), r'\pM', ''))) AS cns,
        data_cadastro,
        TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(cnes_unidade, NFD), r'\pM', ''))) AS cnes_unidade,
        TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(nome_unidade, NFD), r'\pM', ''))) AS nome_unidade,
        data_atualizacao_vinculo_equipe,
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
        TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(nome_pai, NFD), r'\pM', ''))) AS nome_pai,
        updated_at
    FROM `rj-sms.brutos_prontuario_vitacare.paciente`
    -- WHERE paciente_cpf = cpf_filter
),

---------- SMS RIO ----------

smsrio_tb AS (
    SELECT 
        TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(paciente_cpf, NFD), r'\pM', ''))) AS paciente_cpf,
        TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(cns_provisorio, NFD), r'\pM', ''))) AS cns_provisorio,
        TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(telefones, NFD), r'\pM', ''))) AS telefones,
        TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(email, NFD), r'\pM', ''))) AS email,
        timestamp,
        TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(end_logrado, NFD), r'\pM', ''))) AS end_logrado,
        TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(end_cep, NFD), r'\pM', ''))) AS end_cep,
        TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(end_tp_logrado_cod, NFD), r'\pM', ''))) AS end_tp_logrado_cod,
        TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(end_numero, NFD), r'\pM', ''))) AS end_numero,
        TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(end_complem, NFD), r'\pM', ''))) AS end_complem,
        TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(end_bairro, NFD), r'\pM', ''))) AS end_bairro,
        TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(cod_mun_res, NFD), r'\pM', ''))) AS cod_mun_res,
        TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(uf_res, NFD), r'\pM', ''))) AS uf_res,
        dt_nasc,
        TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(sexo, NFD), r'\pM', ''))) AS sexo,
        TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(raca_cor, NFD), r'\pM', ''))) AS raca_cor,
        TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(obito, NFD), r'\pM', ''))) AS obito,
        dt_obito,
        TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(nome_mae, NFD), r'\pM', ''))) AS nome_mae,
        TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(nome_pai, NFD), r'\pM', ''))) AS nome_pai,
        TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(nome, NFD), r'\pM', ''))) AS nome,
        updated_at
    FROM `rj-sms.brutos_plataforma_smsrio.paciente`
    -- WHERE paciente_cpf = cpf_filter
),


-- CNS VITACARE

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


-- CNS SMSRIO

smsrio_cns_ranked AS (
    SELECT
        paciente_cpf,
        TRIM(cns) AS cns,
        ROW_NUMBER() OVER (PARTITION BY paciente_cpf ORDER BY paciente_cpf DESC) AS rank
    FROM (
            SELECT
                paciente_cpf,
                cns,
                timestamp
            FROM smsrio_tb,
            UNNEST(SPLIT(REPLACE(REPLACE(REPLACE(cns_provisorio, '[', ''), ']', ''), '"', ''), ',')) AS cns
    )
    GROUP BY paciente_cpf, TRIM(cns)
    
),


-- CLINICA DA FAMILIA VITACARE
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

-- EQUIPE SAUDE FAMILIA VITACARE

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


-- CONTATO VITACARE

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

-- CONTATO SMSRIO

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
            ROW_NUMBER() OVER (PARTITION BY paciente_cpf ORDER BY timestamp DESC) AS rank
        FROM (
            SELECT
                paciente_cpf,
                telefones,
                timestamp
            FROM smsrio_tb,
            UNNEST(SPLIT(REPLACE(REPLACE(REPLACE(telefones, '[', ''), ']', ''), '"', ''), ',')) AS telefones
        )
        GROUP BY
            paciente_cpf, telefones, timestamp
    )
    WHERE NOT (TRIM(valor) IN ("NONE", "NULL", "") AND (rank >= 2))
),

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
            ROW_NUMBER() OVER (PARTITION BY paciente_cpf ORDER BY timestamp DESC) AS rank
        FROM smsrio_tb
        GROUP BY
            paciente_cpf, email, timestamp
    )
    WHERE NOT (TRIM(valor) IN ("NONE", "NULL", "") AND (rank >= 2))
),

--  ENDEREÇO VITACARE

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

--  ENDEREÇO SMSRIO

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
        timestamp AS datahora_ultima_atualizacao,
        ROW_NUMBER() OVER (PARTITION BY paciente_cpf ORDER BY timestamp DESC) AS rank
    FROM smsrio_tb
    GROUP BY
        paciente_cpf, end_cep, end_tp_logrado_cod, end_logrado, end_numero, end_complem, end_bairro, cod_mun_res, uf_res, timestamp
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


-- PRONTUARIO SMSRIO

smsrio_prontuario AS (
    SELECT
        paciente_cpf,
        'SMSRIO' AS sistema,
        NULL AS id_cnes,
        paciente_cpf AS id_paciente,
        ROW_NUMBER() OVER (PARTITION BY paciente_cpf ORDER BY timestamp DESC) AS rank
    FROM smsrio_tb
    GROUP BY
        paciente_cpf, cod_mun_res, timestamp
),


-- PACIENTE DADOS VITACARE

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

-- PACIENTE DADOS SMSRIO

smsrio_paciente AS (
    SELECT
        paciente_cpf,
        nome,
        "" AS nome_social,
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

-- MERGE DATA

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

---- FINAL JOIN


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