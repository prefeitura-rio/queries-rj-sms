{{
    config(
        schema="brutos_prontuario_vitacare_staging",
        alias="_base_ficha_a_continuo",
        materialized="incremental",
        unique_key="id",
    )
}}

{% set seven_days_ago = (
    modules.datetime.date.today() - modules.datetime.timedelta(days=7)
).isoformat() %}

with

    source as (
        select *, 
                concat(nullif(payload_cnes, ''), '.', nullif(source_id, '')) as id
            from {{ source("brutos_prontuario_vitacare_staging", "paciente_continuo") }}
            {% if is_incremental() %} where source_updated_at > '{{seven_days_ago}}' {% endif %}
    ),


    latest_events as (
        select
            *
        from source
        qualify 
            row_number() over (partition by id order by source_updated_at desc) = 1   
    ),

    ficha_a_continuo as (
        select
            JSON_EXTRACT_SCALAR(data, '$.cpf') AS cpf,
            JSON_EXTRACT_SCALAR(data, '$.id') AS id_paciente,
            nullif(JSON_EXTRACT_SCALAR(data, '$.nPront'), '') AS numero_prontuario,

            nullif(JSON_EXTRACT_SCALAR(data, '$.cnes'), '') AS unidade_cadastro,
            nullif(JSON_EXTRACT_SCALAR(data, '$.ap'), '') AS ap_cadastro,

            JSON_EXTRACT_SCALAR(data, '$.nome') AS nome,
            JSON_EXTRACT_SCALAR(data, '$.sexo') AS sexo,
            JSON_EXTRACT_SCALAR(data, '$.obito') AS obito,
            JSON_EXTRACT_SCALAR(data, '$.bairro') AS bairro,
            JSON_EXTRACT_SCALAR(data, '$.comodos') AS comodos,
            JSON_EXTRACT_SCALAR(data, '$.nomeMae') AS nome_mae,
            JSON_EXTRACT_SCALAR(data, '$.nomePai') AS nome_pai,
            JSON_EXTRACT_SCALAR(data, '$.racaCor') AS raca_cor,
            JSON_EXTRACT_SCALAR(data, '$.ocupacao') AS ocupacao,
            JSON_EXTRACT_SCALAR(data, '$.religiao') AS religiao,
            JSON_EXTRACT_SCALAR(data, '$.telefone') AS telefone,
            JSON_EXTRACT_SCALAR(data, '$.ineEquipe') AS ine_equipe,
            JSON_EXTRACT_SCALAR(data, '$.microarea') AS microarea,
            JSON_EXTRACT_SCALAR(data, '$.logradouro') AS logradouro,
            JSON_EXTRACT_SCALAR(data, '$.nomeSocial') AS nomeSocial,
            JSON_EXTRACT_SCALAR(data, '$.destinoLixo') AS destinoLixo,
            JSON_EXTRACT_SCALAR(data, '$.luzEletrica') AS luz_eletrica,
            JSON_EXTRACT_SCALAR(data, '$.codigoEquipe') AS codigo_equipe,
            JSON_EXTRACT_SCALAR(data, '$.dataCadastro') AS data_cadastro,
            JSON_EXTRACT_SCALAR(data, '$.escolaridade') AS escolaridade,
            JSON_EXTRACT_SCALAR(data, '$.tempoMoradia') AS tempo_moradia,
            JSON_EXTRACT_SCALAR(data, '$.nacionalidade') AS nacionalidade,
            JSON_EXTRACT_SCALAR(data, '$.rendaFamiliar') AS renda_familiar,
            JSON_EXTRACT_SCALAR(data, '$.tipoDomicilio') AS tipo_domicilio,
            JSON_EXTRACT_SCALAR(data, '$.dataNascimento') AS data_nascimento,
            JSON_EXTRACT_SCALAR(data, '$.paisNascimento') AS pais_nascimento,
            JSON_EXTRACT_SCALAR(data, '$.tipoLogradouro') AS tipo_logradouro,
            JSON_EXTRACT_SCALAR(data, '$.tratamentoAgua') AS tratamento_agua,
            JSON_EXTRACT_SCALAR(data, '$.emSituacaoDeRua') AS em_situacao_de_rua,
            JSON_EXTRACT_SCALAR(data, '$.frequentaEscola') AS frequenta_escola,
            ARRAY_TO_STRING(JSON_EXTRACT_ARRAY(data, '$.meiosTransporte'), ', ') AS meios_transporte,
            JSON_EXTRACT_SCALAR(data, '$.situacaoUsuario') AS situacao_usuario,
            ARRAY_TO_STRING(JSON_EXTRACT_ARRAY(data, '$.doencasCondicoes'), ', ') as doencas_condicoes,
            JSON_EXTRACT_SCALAR(data, '$.estadoNascimento') AS estado_nascimento,
            JSON_EXTRACT_SCALAR(data, '$.estadoResidencia') AS estado_residencia,
            JSON_EXTRACT_SCALAR(data, '$.identidadeGenero') AS identidade_genero,
            ARRAY_TO_STRING(JSON_EXTRACT_ARRAY(data, '$.meiosComunicacao'), ', ') AS meios_comunicacao,
            JSON_EXTRACT_SCALAR(data, '$.orientacaoSexual') AS orientacao_sexual,
            JSON_EXTRACT_SCALAR(data, '$.possuiFiltroAgua') AS possui_filtro_agua,
            JSON_EXTRACT_SCALAR(data, '$.possuiPlanoSaude') AS possui_plano_saude,
            JSON_EXTRACT_SCALAR(data, '$.situacaoFamiliar') AS situacao_familiar,
            JSON_EXTRACT_SCALAR(data, '$.territorioSocial') AS territorio_social,
            JSON_EXTRACT_SCALAR(data, '$.abastecimentoAgua') AS abastecimento_agua,
            JSON_EXTRACT_SCALAR(data, '$.animaisNoDomicilio') AS animais_no_domicilio,
            JSON_EXTRACT_SCALAR(data, '$.cadastroPermanente') AS cadastro_permanente,
            JSON_EXTRACT_SCALAR(data, '$.familiaLocalizacao') AS familia_localizacao,
            ARRAY_TO_STRING(JSON_EXTRACT_ARRAY(data, '$.emCasoDoencaProcura'), ', ') AS em_caso_doenca_procura,
            JSON_EXTRACT_SCALAR(data, '$.municipioNascimento') AS municipio_nascimento,
            JSON_EXTRACT_SCALAR(data, '$.municipioResidencia') AS municipio_residencia,
            JSON_EXTRACT_SCALAR(data, '$.responsavelFamiliar') AS responsavel_familiar,
            JSON_EXTRACT_SCALAR(data, '$.esgotamentoSanitario') AS esgotamento_sanitario,
            JSON_EXTRACT_SCALAR(data, '$.situacaoMoradiaPosse') AS situacao_moradia_posse,
            JSON_EXTRACT_SCALAR(data, '$.situacaoProfissional') AS situacao_profissional,
            JSON_EXTRACT_SCALAR(data, '$.vulnerabilidadeSocial') AS vulnerabilidade_social,
            JSON_EXTRACT_SCALAR(data, '$.familiaBeneficiariaCfc') AS familia_beneficiaria_cfc,
            JSON_EXTRACT_SCALAR(data, '$.dataAtualizacaoCadastro') AS data_atualizacao_cadastro,
            JSON_EXTRACT_SCALAR(data, '$.participaGrupoComunitario') AS participa_grupo_comunitario,
            JSON_EXTRACT_SCALAR(data, '$.relacaoResponsavelFamiliar') AS relacao_responsavel_familiar,
            JSON_EXTRACT_SCALAR(data, '$.membroComunidadeTradicional') AS membro_comunidade_tradicional,
            JSON_EXTRACT_SCALAR(data, '$.dataAtualizacaoVinculoEquipe') AS data_atualizacao_vinculo_equipe,
            JSON_EXTRACT_SCALAR(data, '$.familiaBeneficiariaAuxilioBrasil') AS familia_beneficiaria_auxilio_brasil,
            JSON_EXTRACT_SCALAR(data, '$.criancaMatriculadaCrechePreEscola') AS crianca_matriculada_creche_pre_escola,

            source_updated_at as updated_at,
            cast(datalake_loaded_at as string) as loaded_at

        from latest_events
    )

select * from ficha_a_continuo