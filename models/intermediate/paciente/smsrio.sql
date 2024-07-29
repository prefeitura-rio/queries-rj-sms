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
    FROM rj-sms.brutos_plataforma_smsrio.paciente
),


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
    WHERE TRIM(telefones) NOT IN ("NULL","NONE")
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
)

SELECT
    spd.paciente_cpf,
    ARRAY_AGG(STRUCT(scr.cns AS valor, scr.rank)) AS cns,
    ARRAY_AGG(STRUCT(
        spd.paciente_cpf,
        spd.nome,
        spd.nome_social,
        spd.cpf,
        spd.data_nascimento,
        spd.genero,
        spd.raca,
        spd.obito_indicador,
        spd.mae_nome,
        spd.pai_nome,
        spd.rank
    )) AS dados,
    ARRAY_AGG(STRUCT(scf.id_cnes, scf.nome, scf.datahora_ultima_atualizacao, scf.rank)) AS clinica_familia,
    ARRAY_AGG(STRUCT(sesf.id_ine, sesf.datahora_ultima_atualizacao, sesf.rank)) AS equipe_saude_familia,
    ARRAY_AGG(STRUCT(sct.tipo, sct.valor, sct.rank)) AS contato,
    ARRAY_AGG(STRUCT(sed.cep, sed.tipo_logradouro, sed.logradouro, sed.numero, sed.complemento, sed.bairro, sed.cidade, sed.estado, sed.datahora_ultima_atualizacao, sed.rank)) AS endereco,
    ARRAY_AGG(STRUCT(spt.fornecedor, spt.id_cnes, spt.id_paciente, spt.rank)) AS prontuario,
    STRUCT(CURRENT_TIMESTAMP() AS data_geracao) AS metadados
FROM
    smsrio_paciente_dados spd
LEFT JOIN smsrio_cns_ranked scr ON spd.paciente_cpf = scr.paciente_cpf
LEFT JOIN smsrio_clinica_familia scf ON spd.paciente_cpf = scf.paciente_cpf
LEFT JOIN smsrio_equipe_saude_familia sesf ON spd.paciente_cpf = sesf.paciente_cpf
LEFT JOIN smsrio_contato sct ON spd.paciente_cpf = sct.paciente_cpf
LEFT JOIN smsrio_endereco sed ON spd.paciente_cpf = sed.paciente_cpf
LEFT JOIN smsrio_prontuario spt ON spd.paciente_cpf = spt.paciente_cpf
GROUP BY
    spd.paciente_cpf
