WITH smrio_tb AS (
    SELECT 
        paciente_cpf,
        cns_provisorio,
        telefones,
        email,
        timestamp,
        end_logrado,
        end_cep,
        end_tp_logrado_cod,
        end_numero,
        end_complem,
        end_bairro,
        cod_mun_res,
        uf_res,
        dt_nasc,
        sexo,
        raca_cor,
        obito,
        dt_obito,
        nome_mae,
        nome_pai,
        nome,
        updated_at
    FROM rj-sms.brutos_plataforma_smsrio.paciente
    LIMIT 1000
),


smrio_cns_ranked AS (
    SELECT
        paciente_cpf,
        TRIM(cns) AS cns,
        ROW_NUMBER() OVER (PARTITION BY paciente_cpf ORDER BY timestamp DESC) AS rank
    FROM (
        SELECT
            paciente_cpf,
            cns,
            timestamp
    FROM smrio_tb,
            UNNEST(SPLIT(REPLACE(REPLACE(REPLACE(cns_provisorio, '[', ''), ']', ''), '"', ''), ',')) AS cns
        WHERE
            cns_provisorio IS NOT NULL
    )
),

smrio_clinica_familia AS (
    SELECT
        paciente_cpf,
        cod_mun_res AS id_cnes,
        end_logrado AS nome,
        updated_at AS datahora_ultima_atualizacao,
        ROW_NUMBER() OVER (PARTITION BY paciente_cpf ORDER BY updated_at DESC) AS rank
    FROM
        rj-sms.brutos_plataforma_smsrio.paciente
    WHERE
        end_logrado IS NOT NULL
    GROUP BY
        paciente_cpf, cod_mun_res, end_logrado, updated_at
),

smrio_equipe_saude_familia AS (
    SELECT
        paciente_cpf,
        cod_mun_res AS id_ine,
        updated_at AS datahora_ultima_atualizacao,
        ROW_NUMBER() OVER (PARTITION BY paciente_cpf ORDER BY updated_at DESC) AS rank
    FROM
        rj-sms.brutos_plataforma_smsrio.paciente
    WHERE
        cod_mun_res IS NOT NULL
    GROUP BY
        paciente_cpf, cod_mun_res, updated_at
),

smrio_contato AS (
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
    FROM smrio_tb,
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
    FROM
        rj-sms.brutos_plataforma_smsrio.paciente
    WHERE
        email IS NOT NULL
    GROUP BY
        paciente_cpf, email, timestamp
),

smrio_endereco AS (
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
    FROM
        rj-sms.brutos_plataforma_smsrio.paciente
    WHERE
        end_logrado IS NOT NULL
    GROUP BY
        paciente_cpf, end_cep, end_tp_logrado_cod, end_logrado, end_numero, end_complem, end_bairro, cod_mun_res, uf_res, timestamp
),

smrio_prontuario AS (
    SELECT
        paciente_cpf,
        'smrio' AS fornecedor,
        cod_mun_res AS id_cnes,
        NULL AS id_paciente,
        ROW_NUMBER() OVER (PARTITION BY paciente_cpf ORDER BY timestamp DESC) AS rank
    FROM
        rj-sms.brutos_plataforma_smsrio.paciente
    GROUP BY
        paciente_cpf, cod_mun_res, timestamp
),

smrio_paciente_dados AS (
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
    FROM
        rj-sms.brutos_plataforma_smsrio.paciente
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
    smrio_paciente_dados spd
LEFT JOIN smrio_cns_ranked scr ON spd.paciente_cpf = scr.paciente_cpf
LEFT JOIN smrio_clinica_familia scf ON spd.paciente_cpf = scf.paciente_cpf
LEFT JOIN smrio_equipe_saude_familia sesf ON spd.paciente_cpf = sesf.paciente_cpf
LEFT JOIN smrio_contato sct ON spd.paciente_cpf = sct.paciente_cpf
LEFT JOIN smrio_endereco sed ON spd.paciente_cpf = sed.paciente_cpf
LEFT JOIN smrio_prontuario spt ON spd.paciente_cpf = spt.paciente_cpf
GROUP BY
    spd.paciente_cpf
