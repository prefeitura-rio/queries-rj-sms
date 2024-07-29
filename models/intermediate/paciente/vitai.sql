WITH vitai_tb AS (
    SELECT 
        cpf,
        cns,
        cliente,
        nome,
        telefone,
        tipo_logradouro,
        nome_logradouro,
        numero,
        complemento,
        bairro,
        municipio,
        uf,
        updated_at,
        id_cidadao,
        nome_alternativo,
        data_nascimento,
        sexo,
        raca_cor,
        data_obito,
        nome_mae
    FROM `rj-sms.brutos_prontuario_vitai.paciente`
    WHERE cpf NOT NULL AND cpf != "None"
    LIMIT 1000
),

vitai_cns_ranked AS (
    SELECT
        cpf AS paciente_cpf,
        cns,
        ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY updated_at DESC) AS rank
    FROM vitai_tb
    WHERE
        cns IS NOT NULL
),

vitai_clinica_familia AS (
    SELECT
        cpf AS paciente_cpf,
        cliente AS id_cnes,
        cliente AS nome,
        updated_at AS datahora_ultima_atualizacao,
        ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY updated_at DESC) AS rank
    FROM vitai_tb
    WHERE
        nome IS NOT NULL
    GROUP BY
        cpf, cliente, nome, updated_at
),

vitai_equipe_saude_familia AS (
    SELECT
        cpf AS paciente_cpf,
        cliente AS id_ine,
        updated_at AS datahora_ultima_atualizacao,
        ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY updated_at DESC) AS rank
    FROM vitai_tb
    WHERE
        cliente IS NOT NULL
    GROUP BY
        cpf, cliente, updated_at
),

vitai_contato AS (
    SELECT
        cpf AS paciente_cpf,
        'telefone' AS tipo,
        telefone AS valor,
        ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY updated_at DESC) AS rank
    FROM vitai_tb
    WHERE
        telefone IS NOT NULL
    GROUP BY
        cpf, telefone, updated_at
    UNION ALL
    SELECT
        cpf AS paciente_cpf,
        'email' AS tipo,
        '' AS valor, 
        ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY updated_at DESC) AS rank
    FROM vitai_tb
    GROUP BY
        cpf, updated_at
),

vitai_endereco AS (
    SELECT
        cpf AS paciente_cpf,
        '' AS cep, -- Assumindo que a coluna cep não existe na tabela vitai
        tipo_logradouro,
        nome_logradouro AS logradouro,
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

vitai_prontuario AS (
    SELECT
        cpf AS paciente_cpf,
        'vitai' AS fornecedor,
        cliente AS id_cnes,
        id_cidadao AS id_paciente,
        ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY updated_at DESC) AS rank
    FROM vitai_tb
    GROUP BY
        cpf, cliente, id_cidadao, updated_at
),

vitai_paciente_dados AS (
    SELECT
        cpf AS paciente_cpf,
        nome,
        nome_alternativo AS nome_social,
        cpf,
        data_nascimento,
        sexo AS genero,
        raca_cor AS raca,
        data_obito AS obito_indicador,
        NULL AS obito_data,
        nome_mae AS mae_nome,
        NULL AS pai_nome, -- Assumindo que a coluna nome_pai não existe na tabela vitai
        ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY cpf) AS rank
    FROM vitai_tb
    GROUP BY
        cpf, nome, nome_alternativo, cpf, data_nascimento, sexo, raca_cor, data_obito, nome_mae
)

SELECT
    vi_pd.paciente_cpf,
    ARRAY_AGG(STRUCT(vi_cr.cns AS valor, vi_cr.rank)) AS cns,
    ARRAY_AGG(STRUCT(
        vi_pd.paciente_cpf,
        vi_pd.nome,
        vi_pd.nome_social,
        vi_pd.cpf,
        vi_pd.data_nascimento,
        vi_pd.genero,
        vi_pd.raca,
        vi_pd.obito_indicador,
        vi_pd.mae_nome,
        vi_pd.pai_nome,
        vi_pd.rank
    )) AS dados,
    ARRAY_AGG(STRUCT(vi_cf.id_cnes, vi_cf.nome, vi_cf.datahora_ultima_atualizacao, vi_cf.rank)) AS clinica_familia,
    ARRAY_AGG(STRUCT(vi_esf.id_ine, vi_esf.datahora_ultima_atualizacao, vi_esf.rank)) AS equipe_saude_familia,
    ARRAY_AGG(STRUCT(vi_ct.tipo, vi_ct.valor, vi_ct.rank)) AS contato,
    ARRAY_AGG(STRUCT(vi_ed.cep, vi_ed.tipo_logradouro, vi_ed.logradouro, vi_ed.numero, vi_ed.complemento, vi_ed.bairro, vi_ed.cidade, vi_ed.estado, vi_ed.datahora_ultima_atualizacao, vi_ed.rank)) AS endereco,
    ARRAY_AGG(STRUCT(vi_pt.fornecedor, vi_pt.id_cnes, vi_pt.id_paciente, vi_pt.rank)) AS prontuario,
    STRUCT(CURRENT_TIMESTAMP() AS data_geracao) AS metadados
FROM
    vitai_paciente_dados vi_pd
LEFT JOIN vitai_cns_ranked vi_cr ON vi_pd.paciente_cpf = vi_cr.paciente_cpf
LEFT JOIN vitai_clinica_familia vi_cf ON vi_pd.paciente_cpf = vi_cf.paciente_cpf
LEFT JOIN vitai_equipe_saude_familia vi_esf ON vi_pd.paciente_cpf = vi_esf.paciente_cpf
LEFT JOIN vitai_contato vi_ct ON vi_pd.paciente_cpf = vi_ct.paciente_cpf
LEFT JOIN vitai_endereco vi_ed ON vi_pd.paciente_cpf = vi_ed.paciente_cpf
LEFT JOIN vitai_prontuario vi_pt ON vi_pd.paciente_cpf = vi_pt.paciente_cpf
GROUP BY
    vi_pd.paciente_cpf
