-- models/ficha_a.sql
{{
    config(
        alias="ficha_a",
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

    selecao_ficha_a AS (
        SELECT
            -- PK
            CONCAT(
                NULLIF(id_cnes, ''),
                '.',
                NULLIF(ut_id, '')
            ) AS id,
            cpf,
            ut_id AS id_paciente,
            npront AS numero_prontuario,

            unidade AS unidade_cadastro,
            ap AS ap_cadastro,

            {{ proper_br('nome') }} AS nome,
            sexo,
            obito,
            bairro,
            comodos,
            {{ proper_br('nome_mae') }} AS nome_mae,
            {{ proper_br('nome_pai') }} AS nome_pai,
            raca_cor,
            ocupacao,
            religiao,
            telefone,
            ine_equipe,
            microarea,
            logradouro,
            nome_social,
            destino_lixo,
            luz_eletrica,
            codigo_equipe,
            data_cadastro,
            escolaridade,
            tempo_moradia,
            nacionalidade,
            renda_familiar,
            tipo_domicilio,
            data_nascimento,
            pais_nascimento,
            tipo_logradouro,
            tratamento_agua,
            em_situacao_de_rua,
            frequenta_escola,
            meios_transporte,
            situacao_usuario,
            doencas_condicoes,
            estado_nascimento,
            estado_residencia,
            identidade_genero,
            meios_comunicacao,
            orientacao_sexual,
            possui_filtro_agua,
            possui_plano_saude,
            situacao_familiar,
            territorio_social,
            abastecimento_agua,
            animais_no_domicilio,
            cadastro_permanente,
            familia_localizacao,
            em_caso_doenca_procura,
            municipio_nascimento,
            municipio_residencia,
            responsavel_familiar,
            esgotamento_sanitario,
            situacao_moradia_posse,
            situacao_profissional,
            vulnerabilidade_social,
            familia_beneficiaria_cfc,
            data_atualizacao_cadastro,
            participa_grupo_comunitario,
            relacao_responsavel_familiar,
            membro_comunidade_tradicional,
            data_atualizacao_vinculo_equipe,
            familia_beneficiaria_auxilio_brasil,
            crianca_matriculada_creche_pre_escola,
            updated_at,
            loaded_at,
            data_particao

        FROM source_cadastro
    )

SELECT
    *
FROM selecao_ficha_a