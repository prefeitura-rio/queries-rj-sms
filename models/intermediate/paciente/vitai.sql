DECLARE max_rank INT64 DEFAULT 3;

WITH vitai_tb AS (
    SELECT 
        TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(cpf, NFD), r'\pM', ''))) AS cpf,
        TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(cns, NFD), r'\pM', ''))) AS cns,
        TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(cliente, NFD), r'\pM', ''))) AS cliente,
        TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(nome, NFD), r'\pM', ''))) AS nome,
        TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(telefone, NFD), r'\pM', ''))) AS telefone,
        TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(tipo_logradouro, NFD), r'\pM', ''))) AS tipo_logradouro,
        TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(nome_logradouro, NFD), r'\pM', ''))) AS nome_logradouro,
        TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(numero, NFD), r'\pM', ''))) AS numero,
        TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(complemento, NFD), r'\pM', ''))) AS complemento,
        TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(bairro, NFD), r'\pM', ''))) AS bairro,
        TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(municipio, NFD), r'\pM', ''))) AS municipio,
        TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(uf, NFD), r'\pM', ''))) AS uf,
        updated_at,
        TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(id_cidadao, NFD), r'\pM', ''))) AS id_cidadao,
        TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(nome_alternativo, NFD), r'\pM', ''))) AS nome_alternativo,
        data_nascimento,
        TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(sexo, NFD), r'\pM', ''))) AS sexo,
        TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(raca_cor, NFD), r'\pM', ''))) AS raca_cor,
        data_obito,
        TRIM(UPPER(REGEXP_REPLACE(NORMALIZE(nome_mae, NFD), r'\pM', ''))) AS nome_mae
    FROM `rj-sms.brutos_prontuario_vitai.paciente`
    WHERE cpf IS NOT NULL
        AND NOT REGEXP_CONTAINS(cpf, r'[A-Za-z]')
        AND TRIM(cpf) != ""
),

vitai_cns_ranked AS (
    SELECT
        cpf AS paciente_cpf,
        cns,
        ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY cpf DESC) AS rank
    FROM vitai_tb
    WHERE
        cns IS NOT NULL
        AND TRIM(cns) NOT IN ("")
    GROUP BY cpf, cns
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
        ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY cpf DESC) AS rank
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
        ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY cpf DESC) AS rank
    FROM vitai_tb
    GROUP BY
        cpf
),

vitai_endereco AS (
    SELECT
        cpf AS paciente_cpf,
        '' AS cep,
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
        'VITAI' AS fornecedor,
        cliente AS id_cnes,
        cpf AS id_paciente,
        ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY cpf DESC) AS rank
    FROM vitai_tb
    GROUP BY
        cpf, cliente
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
LEFT JOIN vitai_cns_ranked vi_cr ON vi_pd.paciente_cpf = vi_cr.paciente_cpf AND vi_cr.rank < max_rank
LEFT JOIN vitai_clinica_familia vi_cf ON vi_pd.paciente_cpf = vi_cf.paciente_cpf AND vi_cf.rank < max_rank
LEFT JOIN vitai_equipe_saude_familia vi_esf ON vi_pd.paciente_cpf = vi_esf.paciente_cpf AND vi_esf.rank < max_rank
LEFT JOIN vitai_contato vi_ct ON vi_pd.paciente_cpf = vi_ct.paciente_cpf AND vi_ct.rank < max_rank
LEFT JOIN vitai_endereco vi_ed ON vi_pd.paciente_cpf = vi_ed.paciente_cpf AND vi_ed.rank < max_rank
LEFT JOIN vitai_prontuario vi_pt ON vi_pd.paciente_cpf = vi_pt.paciente_cpf AND vi_pt.rank < max_rank
GROUP BY
    vi_pd.paciente_cpf
