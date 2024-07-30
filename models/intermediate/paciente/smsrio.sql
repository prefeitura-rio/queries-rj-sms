WITH smsrio_tb AS (
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
),

-- CNS

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
            WHERE
                cns_provisorio IS NOT NULL
    )
    GROUP BY paciente_cpf, TRIM(cns)
    
),

cns_dados AS (
    SELECT
        paciente_cpf,
        ARRAY_AGG(STRUCT(
            cns AS valor, 
            rank
        )) AS cns
    FROM smsrio_cns_ranked 
    GROUP BY paciente_cpf
),


-- CLINICA DA FAMILIA

smsrio_clinica_familia AS (
    SELECT
        paciente_cpf,
        cod_mun_res AS id_cnes,
        end_logrado AS nome,
        updated_at AS datahora_ultima_atualizacao,
        ROW_NUMBER() OVER (PARTITION BY paciente_cpf ORDER BY updated_at DESC) AS rank
    FROM smsrio_tb
    WHERE
        end_logrado IS NOT NULL
    GROUP BY
        paciente_cpf, cod_mun_res, end_logrado, updated_at
),

clinica_familia_dados AS (
    SELECT
        paciente_cpf,
        ARRAY_AGG(STRUCT(
            id_cnes, 
            nome, 
            datahora_ultima_atualizacao, 
            rank
        )) AS clinica_familia
    FROM smsrio_clinica_familia
    GROUP BY paciente_cpf
),


-- EQUIPE SAUDE FAMILIA

smsrio_equipe_saude_familia AS (
    SELECT
        paciente_cpf,
        cod_mun_res AS id_ine,
        updated_at AS datahora_ultima_atualizacao,
        ROW_NUMBER() OVER (PARTITION BY paciente_cpf ORDER BY updated_at DESC) AS rank
    FROM smsrio_tb
    WHERE
        cod_mun_res IS NOT NULL
    GROUP BY
        paciente_cpf, cod_mun_res, updated_at
),

equipe_saude_familia_dados AS (
    SELECT
        paciente_cpf,
        ARRAY_AGG(STRUCT(
            id_ine, 
            datahora_ultima_atualizacao, 
            rank
        )) AS equipe_saude_familia
    FROM smsrio_equipe_saude_familia
    GROUP BY paciente_cpf
),

-- EQUIPE CONTATO

smsrio_contato AS (
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
        WHERE
            telefones IS NOT NULL
    )
    GROUP BY
        paciente_cpf, telefones, timestamp
    UNION ALL
    SELECT
        paciente_cpf,
        'email' AS tipo,
        email AS valor,
        ROW_NUMBER() OVER (PARTITION BY paciente_cpf ORDER BY timestamp DESC) AS rank
    FROM smsrio_tb
    WHERE
        email IS NOT NULL
    GROUP BY
        paciente_cpf, email, timestamp
),

contato_dados AS (
    SELECT
        paciente_cpf,
        ARRAY_AGG(STRUCT(
            tipo, 
            valor, 
            rank
        )) AS contato
    FROM smsrio_contato
    WHERE TRIM(valor) NOT IN ("NONE","", "NULL")
    GROUP BY paciente_cpf
),


-- EQUIPE ENDEREÇO

smsrio_endereco AS (
    SELECT
        paciente_cpf,
        end_cep AS cep,
        end_tp_logrado_cod AS tipo_logradouro,
        end_logrado AS logradouro,
        end_numero AS numero,
        end_complem AS complemento,
        end_bairro AS bairro,
        cod_mun_res AS cidade,
        uf_res AS estado,
        timestamp AS datahora_ultima_atualizacao,
        ROW_NUMBER() OVER (PARTITION BY paciente_cpf ORDER BY timestamp DESC) AS rank
    FROM smsrio_tb
    WHERE
        end_logrado IS NOT NULL
    GROUP BY
        paciente_cpf, end_cep, end_tp_logrado_cod, end_logrado, end_numero, end_complem, end_bairro, cod_mun_res, uf_res, timestamp
),


endereco_dados AS (
    SELECT
        paciente_cpf,
        ARRAY_AGG(STRUCT(
            cep, 
            tipo_logradouro, 
            logradouro, numero, 
            complemento, 
            bairro, 
            cidade, 
            estado, 
            datahora_ultima_atualizacao, 
            rank
        )) AS endereco
    FROM smsrio_endereco
    GROUP BY paciente_cpf
),

-- PRONTUARIO

smsrio_prontuario AS (
    SELECT
        paciente_cpf,
        'SMSRIO' AS fornecedor,
        cod_mun_res AS id_cnes,
        NULL AS id_paciente,
        ROW_NUMBER() OVER (PARTITION BY paciente_cpf ORDER BY timestamp DESC) AS rank
    FROM smsrio_tb
    GROUP BY
        paciente_cpf, cod_mun_res, timestamp
),

prontuario_dados AS (
    SELECT
        paciente_cpf,
        ARRAY_AGG(STRUCT(
            fornecedor, 
            id_cnes, 
            id_paciente, 
            rank
        )) AS prontuario
    FROM smsrio_prontuario
    GROUP BY paciente_cpf
),

-- PACIENTE DADOS

smsrio_paciente_dados AS (
    SELECT
        paciente_cpf,
        nome,
        NULL AS nome_social,
        paciente_cpf AS cpf,
        dt_nasc AS data_nascimento,
        sexo AS genero,
        raca_cor AS raca,
        obito as obito_indicador,
        dt_obito AS obito_data,
        nome_mae AS mae_nome,
        nome_pai AS pai_nome,
        ROW_NUMBER() OVER (PARTITION BY paciente_cpf ORDER BY paciente_cpf) AS rank
    FROM smsrio_tb
    GROUP BY
        paciente_cpf, nome, dt_nasc, sexo, raca_cor, obito, dt_obito, nome_mae, nome_pai
),

paciente_dados AS (
    SELECT
        paciente_cpf,
        ARRAY_AGG(STRUCT(
            nome,
            nome_social,
            cpf,
            data_nascimento,
            genero,
            raca,
            obito_indicador,
            mae_nome,
            pai_nome,
            rank
        )) AS dados,
    FROM smsrio_paciente_dados
    GROUP BY
        paciente_cpf
)

-- FINAL JOIN

SELECT
    pd.paciente_cpf,
    cns.cns,
    pd.dados,
    cf.clinica_familia,
    esf.equipe_saude_familia,
    ct.contato,
    ed.endereco,
    pt.prontuario,
    STRUCT(CURRENT_TIMESTAMP() AS data_geracao) AS metadados
FROM paciente_dados pd
LEFT JOIN cns_dados cns ON pd.paciente_cpf = cns.paciente_cpf
LEFT JOIN clinica_familia_dados cf ON pd.paciente_cpf = cf.paciente_cpf
LEFT JOIN equipe_saude_familia_dados esf ON pd.paciente_cpf = esf.paciente_cpf
LEFT JOIN contato_dados ct ON pd.paciente_cpf = ct.paciente_cpf
LEFT JOIN endereco_dados ed ON pd.paciente_cpf = ed.paciente_cpf
LEFT JOIN prontuario_dados pt ON pd.paciente_cpf = pt.paciente_cpf
