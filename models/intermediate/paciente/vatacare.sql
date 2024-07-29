
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

vitacare_contato AS (
    SELECT
        paciente_cpf,
        'telefone' AS tipo,
        telefone AS valor,
        ROW_NUMBER() OVER (PARTITION BY paciente_cpf ORDER BY data_cadastro DESC) AS rank
    FROM vitacare_tb
    WHERE
        telefone IS NOT NULL
    GROUP BY
        paciente_cpf, telefone, data_cadastro
    UNION ALL
    SELECT
        paciente_cpf,
        'email' AS tipo,
        email AS valor,
        ROW_NUMBER() OVER (PARTITION BY paciente_cpf ORDER BY data_cadastro DESC) AS rank
    FROM vitacare_tb
    WHERE
        email IS NOT NULL
    GROUP BY
        paciente_cpf, email, data_cadastro
),

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

vitacare_prontuario AS (
    SELECT
        paciente_cpf,
        'VITACARE' AS fornecedor,
        cnes_unidade AS id_cnes,
        id AS id_paciente,
        ROW_NUMBER() OVER (PARTITION BY paciente_cpf ORDER BY data_cadastro DESC) AS rank
    FROM vitacare_tb
    GROUP BY
        paciente_cpf, cnes_unidade, id, data_cadastro
),

vitacare_paciente_dados AS (
    SELECT
        paciente_cpf,
        nome,
        nome_social,
        cpf,
        data_nascimento,
        sexo AS genero,
        raca_cor AS raca,
        obito as obito_indicador,
        NULL AS obito_data,
        nome_mae AS mae_nome,
        nome_pai AS pai_nome,
        ROW_NUMBER() OVER (PARTITION BY paciente_cpf ORDER BY paciente_cpf) AS rank
    FROM vitacare_tb
    GROUP BY
        paciente_cpf, nome, nome_social, cpf, data_nascimento, sexo, raca_cor, obito, nome_mae, nome_pai
)

SELECT
    vc_pd.paciente_cpf,
    ARRAY_AGG(STRUCT(vc_cr.cns AS valor, vc_cr.rank)) AS cns,
    ARRAY_AGG(STRUCT(
        vc_pd.paciente_cpf,
        vc_pd.nome,
        vc_pd.nome_social,
        vc_pd.cpf,
        vc_pd.data_nascimento,
        vc_pd.genero,
        vc_pd.raca,
        vc_pd.obito_indicador,
        vc_pd.mae_nome,
        vc_pd.pai_nome,
        vc_pd.rank
    )) AS dados,
    ARRAY_AGG(STRUCT(vc_cf.id_cnes, vc_cf.nome, vc_cf.datahora_ultima_atualizacao, vc_cf.rank)) AS clinica_familia,
    ARRAY_AGG(STRUCT(vc_esf.id_ine, vc_esf.datahora_ultima_atualizacao, vc_esf.rank)) AS equipe_saude_familia,
    ARRAY_AGG(STRUCT(vc_ct.tipo, vc_ct.valor, vc_ct.rank)) AS contato,
    ARRAY_AGG(STRUCT(vc_ed.cep, vc_ed.tipo_logradouro, vc_ed.logradouro, vc_ed.numero, vc_ed.complemento, vc_ed.bairro, vc_ed.cidade, vc_ed.estado, vc_ed.datahora_ultima_atualizacao, vc_ed.rank)) AS endereco,
    ARRAY_AGG(STRUCT(vc_pt.fornecedor, vc_pt.id_cnes, vc_pt.id_paciente, vc_pt.rank)) AS prontuario,
    STRUCT(CURRENT_TIMESTAMP() AS data_geracao) AS metadados
FROM
    vitacare_paciente_dados vc_pd
LEFT JOIN vitacare_cns_ranked vc_cr ON vc_pd.paciente_cpf = vc_cr.paciente_cpf
LEFT JOIN vitacare_clinica_familia vc_cf ON vc_pd.paciente_cpf = vc_cf.paciente_cpf
LEFT JOIN vitacare_equipe_saude_familia vc_esf ON vc_pd.paciente_cpf = vc_esf.paciente_cpf
LEFT JOIN vitacare_contato vc_ct ON vc_pd.paciente_cpf = vc_ct.paciente_cpf
LEFT JOIN vitacare_endereco vc_ed ON vc_pd.paciente_cpf = vc_ed.paciente_cpf
LEFT JOIN vitacare_prontuario vc_pt ON vc_pd.paciente_cpf = vc_pt.paciente_cpf
GROUP BY
    vc_pd.paciente_cpf