-- models/ficha_a.sql
{{
    config(
        alias="ficha_a",
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

    selecao_ficha_a AS (
        SELECT
            -- PK
            CONCAT(
                NULLIF(cnes_unidade, ''), 
                '.',
                NULLIF(ut_id, '')
            ) AS id,
            cpf AS cpf,
            ut_id AS id_paciente,
            npront AS numero_prontuario,

            unidade AS unidade_cadastro, 
            ap AS ap_cadastro,

            nome AS nome,
            sexo AS sexo,
            obito AS obito,
            bairro AS bairro,
            comodos AS comodos,
            nomemae AS nome_mae,
            nomepai AS nome_pai,
            racacor AS raca_cor,
            ocupacao AS ocupacao,
            religiao AS religiao,
            telefone AS telefone,
            ineequipe AS ine_equipe,
            microarea AS microarea,
            logradouro AS logradouro,
            nomesocial AS nome_social,
            destinolixo AS destino_lixo,
            luzeletrica AS luz_eletrica,
            codigoequipe AS codigo_equipe,
            datacadastro AS data_cadastro,
            escolaridade AS escolaridade,
            tempomoradia AS tempo_moradia,
            nacionalidade AS nacionalidade,
            rendafamiliar AS renda_familiar,
            tipodomicilio AS tipo_domicilio,
            dta_nasc AS data_nascimento,
            paisnascimento AS pais_nascimento,
            tipologradouro AS tipo_logradouro,
            tratamentoagua AS tratamento_agua,
            emsituacaoderua AS em_situacao_de_rua,
            frequentaescola AS frequenta_escola,
            meiostransporte AS meios_transporte,
            situacaousuario AS situacao_usuario,
            doencascondicoes AS doencas_condicoes,
            estadonascimento AS estado_nascimento,
            estadoresidencia AS estado_residencia,
            identidadegenero AS identidade_genero,
            meioscomunicacao AS meios_comunicacao,
            orientacaosexual AS orientacao_sexual,
            possuifiltroagua AS possui_filtro_agua,
            possuiplanosaude AS possui_plano_saude,
            situacaofamiliar AS situacao_familiar,
            territoriosocial AS territorio_social,
            abastecimentoagua AS abastecimento_agua,
            animaisnodomicilio AS animais_no_domicilio,
            cadastropermanente AS cadastro_permanente,
            familialocalizacao AS familia_localizacao,
            emcasodoencaprocura AS em_caso_doenca_procura,
            municipionascimento AS municipio_nascimento,
            municipioresidencia AS municipio_residencia,
            responsavelfamiliar AS responsavel_familiar,
            esgotamentosanitario AS esgotamento_sanitario,
            situacaomoradiaposse AS situacao_moradia_posse,
            situacaoprofissional AS situacao_profissional,
            vulnerabilidadesocial AS vulnerabilidade_social,
            familiabeneficiariacfc AS familia_beneficiaria_cfc,
            dataatualizacaocadastro AS data_atualizacao_cadastro,
            participagrupocomunitario AS participa_grupo_comunitario,
            relacaoresponsavelfamiliar AS relacao_responsavel_familiar,
            membrocomunidadetradicional AS membro_comunidade_tradicional,
            dataatualizacaovinculoequipe AS data_atualizacao_vinculo_equipe,
            familiabeneficiariaauxiliobrasil AS familia_beneficiaria_auxilio_brasil,
            criancamatriculadacrechepreescola AS crianca_matriculada_creche_pre_escola,
            updated_at AS updated_at,
            loaded_at,
            DATE(SAFE_CAST(loaded_at AS DATETIME)) AS data_particao

        FROM source_cadastro
    )

SELECT
    *
FROM selecao_ficha_a