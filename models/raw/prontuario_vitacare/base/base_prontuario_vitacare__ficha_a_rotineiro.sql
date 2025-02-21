{{
    config(
        schema="brutos_prontuario_vitacare_staging",
        alias="_base_ficha_a_rotineiro",
        materialized="table",
    )
}}


with

    events_from_window as (
        select *, concat(nullif(payload_cnes, ''), '.', nullif(data__id, '')) as id
        from {{ source("brutos_prontuario_vitacare_staging", "paciente_eventos_cloned") }}
    ),

    events_ranked_by_freshness as (
        select
            *,
            row_number() over (partition by id order by source_updated_at desc) as rank
        from events_from_window
    ),

    latest_events as (select * from events_ranked_by_freshness where rank = 1),

    -- -----------------------------------------------------
    -- Ficha A
    -- -----------------------------------------------------
    dados_ficha_a as (
        select
            safe_cast(nullif(patient_cpf, '') as string) as cpf,
            id as id_paciente,
            data__nPront as numero_prontuario,
            
            payload_cnes as unidade_cadastro,
            nullif(data__ap,'') as ap_cadastro,

            {{ process_null('data__nome') }} as nome,
            {{ process_null('data__sexo') }} as sexo,
            {{ process_null('data__obito') }} as obito,
            {{ process_null('data__bairro') }} as bairro,
            {{ process_null('data__comodos') }} as comodos,
            {{ process_null('data__nomeMae') }} as nome_mae,
            {{ process_null('data__nomePai') }} as nome_pai,
            {{ process_null('data__racaCor') }} as raca_cor,
            {{ process_null('data__ocupacao') }} as ocupacao,
            {{ process_null('data__religiao') }} as religiao,
            {{ process_null('data__telefone') }} as telefone,
            {{ process_null('data__ineEquipe') }} as ine_equipe,
            {{ process_null('data__microarea') }} as microarea,
            {{ process_null('data__logradouro') }} as logradouro,
            {{ process_null('data__nomeSocial') }} as nome_social,
            {{ process_null('data__destinoLixo') }} as destino_lixo,
            {{ process_null('data__luzEletrica') }} as luz_eletrica,
            {{ process_null('data__codigoEquipe') }} as codigo_equipe,
            {{ process_null('data__dataCadastro') }} as data_cadastro,
            {{ process_null('data__escolaridade') }} as escolaridade,
            {{ process_null('data__tempoMoradia') }} as tempo_moradia,
            {{ process_null('data__nacionalidade') }} as nacionalidade,
            {{ process_null('data__rendaFamiliar') }} as renda_familiar,
            {{ process_null('data__tipoDomicilio') }} as tipo_domicilio,
            {{ process_null('data__dataNascimento') }} as data_nascimento,
            {{ process_null('data__paisNascimento') }} as pais_nascimento,
            {{ process_null('data__tipoLogradouro') }} as tipo_logradouro,
            {{ process_null('data__tratamentoAgua') }} as tratamento_agua,
            {{ process_null('data__emSituacaoDeRua') }} as em_situacao_de_rua,
            {{ process_null('data__frequentaEscola') }} as frequenta_escola,
            {{ process_null('data__meiosTransporte') }} as meios_transporte,
            {{ process_null('data__situacaoUsuario') }} as situacao_usuario,
            {{ process_null('data__doencasCondicoes') }} as doencas_condicoes,
            {{ process_null('data__estadoNascimento') }} as estado_nascimento,
            {{ process_null('data__estadoResidencia') }} as estado_residencia,
            {{ process_null('data__identidadeGenero') }} as identidade_genero,
            {{ process_null('data__meiosComunicacao') }} as meios_comunicacao,
            {{ process_null('data__orientacaoSexual') }} as orientacao_sexual,
            {{ process_null('data__possuiFiltroAgua') }} as possui_filtro_agua,
            {{ process_null('data__possuiPlanoSaude') }} as possui_plano_saude,
            {{ process_null('data__situacaoFamiliar') }} as situacao_familiar,
            {{ process_null('data__territorioSocial') }} as territorio_social,
            {{ process_null('data__abastecimentoAgua') }} as abastecimento_agua,
            {{ process_null('data__animaisNoDomicilio') }} as animais_no_domicilio,
            {{ process_null('data__cadastroPermanente') }} as cadastro_permanente,
            {{ process_null('data__familiaLocalizacao') }} as familia_localizacao,
            {{ process_null('data__emCasoDoencaProcura') }} as em_caso_doenca_procura,
            {{ process_null('data__municipioNascimento') }} as municipio_nascimento,
            {{ process_null('data__municipioResidencia') }} as municipio_residencia,
            {{ process_null('data__responsavelFamiliar') }} as responsavel_familiar,
            {{ process_null('data__esgotamentoSanitario') }} as esgotamento_sanitario,
            {{ process_null('data__situacaoMoradiaPosse') }} as situacao_moradia_posse,
            {{ process_null('data__situacaoProfissional') }} as situacao_profissional,
            {{ process_null('data__vulnerabilidadeSocial') }} as vulnerabilidade_social,
            {{ process_null('data__familiaBeneficiariaCfc') }} as familia_beneficiaria_cfc,
            {{ process_null('data__dataAtualizacaoCadastro') }} as data_atualizacao_cadastro,
            {{ process_null('data__participaGrupoComunitario') }} as participa_grupo_comunitario,
            {{ process_null('data__relacaoResponsavelFamiliar') }} as relacao_responsavel_familiar,
            {{ process_null('data__membroComunidadeTradicional') }} as membro_comunidade_tradicional,
            {{ process_null('data__dataAtualizacaoVinculoEquipe') }} as data_atualizacao_vinculo_equipe,
            {{ process_null('data__familiaBeneficiariaAuxilioBrasil') }} as familia_beneficiaria_auxilio_brasil,
            {{ process_null('data__criancaMatriculadaCrechePreEscola') }} as crianca_matriculada_creche_pre_escola,

            source_updated_at as updated_at,
            datalake_loaded_at as loaded_at
        from latest_events
    )

select *
from dados_ficha_a
