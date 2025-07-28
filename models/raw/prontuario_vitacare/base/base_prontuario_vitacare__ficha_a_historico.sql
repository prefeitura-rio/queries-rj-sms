{{
    config(
        schema="brutos_prontuario_vitacare_staging",
        alias="_base_ficha_a_historico",
        materialized="table",
    )
}}

with

    source as (
        select
            concat(
                nullif(id_cnes, ''),
                '.',
                nullif({{ clean_numeric_string("ut_id") }}, '')
            ) as id, 
            * 
        from {{ ref("raw_prontuario_vitacare_historico__cadastro") }}
    ),

    dados_ficha_a as (
        select
            cast(id as string) as id,
            
            cast(cpf as string) as cpf,
            ut_id as id_paciente,
            npront as numero_prontuario,
            
            id_cnes as unidade_cadastro
            ap as ap_cadastro,

            nome,
            sexo,
            obito,
            bairro,
            comodos,
            nome_mae,
            nome_pai,
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
            loaded_at
        from source
    )

select *
from dados_ficha_a
