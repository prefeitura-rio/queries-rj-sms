-- models/pacientes.sql
{{
    config(
        alias="paciente",
        materialized="table",
        schema="brutos_prontuario_vitacare_historico",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day"
        } 
    )
}}

WITH

    source_cadastro AS (
        SELECT
            *
        FROM {{ ref('raw_prontuario_vitacare_historico__cadastro') }} 
    ),

    selecao_pacientes AS (
        SELECT
            -- PK
            CONCAT(
                NULLIF(cnes_unidade, ''), 
                '.',
                NULLIF(ut_id, '')
            ) AS id,

            -- Outras Chaves
            cnes_unidade AS id_cnes,
            ut_id AS id_local,
            npront AS numero_prontuario,
            cpf AS cpf,
            dnv AS dnv,
            nis AS nis,
            cns AS cns,

            -- Informações Pessoais
            nome AS nome,
            nomesocial AS nome_social,
            nomemae AS nome_mae,
            nomepai AS nome_pai,
            obito AS obito,
            sexo AS sexo,
            orientacaosexual AS orientacao_sexual,
            identidadegenero AS identidade_genero,
            racacor AS raca_cor,

            -- Informações Cadastrais
            situacaousuario AS situacao,
            cadastropermanente AS cadastro_permanente,
            datacadastro AS data_cadastro_inicial,
            dataatualizacaocadastro AS data_ultima_atualizacao_cadastral,

            -- Nascimento
            nacionalidade AS nacionalidade,
            dta_nasc AS data_nascimento,
            paisnascimento AS pais_nascimento,
            municipionascimento AS municipio_nascimento,
            estadonascimento AS estado_nascimento,

            -- Contato
            email AS email,
            telefone AS telefone,

            -- Endereço
            tipodomicilio AS endereco_tipo_domicilio,
            tipologradouro AS endereco_tipo_logradouro,
            cep AS endereco_cep,
            logradouro AS endereco_logradouro,
            bairro AS endereco_bairro,
            estadoresidencia AS endereco_estado,
            municipioresidencia AS endereco_municipio,

            -- Informações da Unidade
            ap AS ap,
            microarea AS microarea,
            unidade AS nome_unidade, 
            codigoequipe AS codigo_equipe_saude,
            ineequipe AS codigo_ine_equipe_saude,
            dataatualizacaovinculoequipe AS data_atualizacao_vinculo_equipe,


            loaded_at,
            DATE(SAFE_CAST(loaded_at AS DATETIME)) AS data_particao

        FROM source_cadastro
    )

SELECT
    *
FROM selecao_pacientes