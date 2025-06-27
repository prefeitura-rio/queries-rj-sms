-- models/pacientes.sql
{{
    config(
        alias="paciente",
        materialized="incremental",
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
                NULLIF(id_cnes, ''), 
                '.',
                NULLIF(ut_id, '')
            ) AS id,

            -- Outras Chaves
            id_cnes,
            ut_id AS id_local,
            npront AS numero_prontuario,
            cpf,
            dnv,
            nis,
            cns,

            -- Informações Pessoais
            nome,
            nome_social,
            nome_mae,
            nome_pai,
            obito,
            sexo,
            orientacao_sexual,
            identidade_genero,
            raca_cor,

            -- Informações Cadastrais
            situacao_usuario AS situacao,
            cadastro_permanente,
            data_cadastro AS data_cadastro_inicial,
            data_atualizacao_cadastro AS data_ultima_atualizacao_cadastral,

            -- Nascimento
            nacionalidade,
            data_nascimento,
            pais_nascimento,
            municipio_nascimento,
            estado_nascimento,

            -- Contato
            email,
            telefone,

            -- Endereço
            tipo_domicilio AS endereco_tipo_domicilio,
            tipo_logradouro AS endereco_tipo_logradouro,
            cep AS endereco_cep,
            logradouro AS endereco_logradouro,
            bairro AS endereco_bairro,
            estado_residencia AS endereco_estado,
            municipio_residencia AS endereco_municipio,

            -- Informações da Unidade
            ap,
            microarea,
            unidade AS nome_unidade,
            codigo_equipe AS codigo_equipe_saude,
            ine_equipe AS codigo_ine_equipe_saude,
            data_atualizacao_vinculo_equipe,


            loaded_at,
            DATE(SAFE_CAST(loaded_at AS DATETIME)) AS data_particao

        FROM source_cadastro
    )

SELECT
    *
FROM selecao_pacientes