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

            payload_cnes as unidade_cadastro,
            data__ap as ap_cadastro,

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
    ),
    -- -----------------------------------------------------
    -- Padronização
    -- -----------------------------------------------------
    ficha_a_padronizada as (
        select 
            safe_cast(cpf as string) as cpf,
            safe_cast(id_paciente as string) as id_paciente,
            safe_cast(unidade_cadastro as string) as unidade_cadastro,
            regexp_replace(ap_cadastro,'.0','') as ap_cadastro,
            {{ proper_br('nome') }} as nome,
            case 
                when sexo = 'male' then 'masculino'
                when sexo = 'female' then 'feminino'
                else null
            end as sexo,
            safe_cast(obito as bool) as obito,
            safe_cast(bairro as string) as bairro,
            safe_cast(comodos as integer) as comodos,
            case 
                when regexp_replace(lower(nome_mae),r'(sem registro)|(sem informa[c|ç|ã][a|ã|][o|(oes)|(ões)])|(m[ã|a]e desconhecida)|(n[a|ã|][|o] informado)|(n[a|ã]o declarado)|(desconhecido)|(n[a|ã]o declarado)|(n[a|ã]o consta)|(sem inf[|o])','') = ''  then null 
                else {{ proper_br('nome_mae') }} 
            end as nome_mae,
            case 
                when regexp_replace(lower(nome_pai),r'(sem registro)|(sem informa[c|ç|ã][a|ã|][o|(oes)|(ões)])|(m[ã|a]e desconhecida)|(n[a|ã|][|o] informado)|(n[a|ã]o declarado)|(desconhecido)|(n[a|ã]o declarado)|(n[a|ã]o consta)|(sem inf[|o])','') = ''  then null 
                else {{ proper_br('nome_pai') }} 
            end as nome_pai,
            lower(raca_cor) as raca_cor,
            safe_cast(ocupacao as string) as ocupacao,
            safe_cast(religiao as string) as religiao,
            {{ padronize_telefone('telefone') }} as telefone,
            safe_cast(ine_equipe as string) as ine_equipe,
            safe_cast(microarea as string) as microarea,
            nullif(regexp_replace(regexp_replace(logradouro,'^0.*$',''),'null',''),'') as logradouro,
            {{ proper_br('nome_social') }} as nome_social,
            safe_cast(destino_lixo as string) as destino_lixo,
            safe_cast(luz_eletrica as bool) as luz_eletrica,
            safe_cast(codigo_equipe as string) as codigo_equipe,
            timestamp_sub(timestamp(data_cadastro, "Brazil/East"),interval 2 hour) as data_cadastro,
            safe_cast(escolaridade as string) as escolaridade,
            regexp_replace(regexp_replace(regexp_replace(lower(tempo_moradia),r'\+ de','mais de '),r'\+','mais de '),' {2,}',' ') as tempo_moradia,
            lower(nacionalidade) as nacionalidade,
            safe_cast(renda_familiar as string) as renda_familiar,
            safe_cast(tipo_domicilio as string) as tipo_domicilio,
            safe_cast(data_nascimento as date) as data_nascimento,
            safe_cast(pais_nascimento as string) as pais_nascimento,
            safe_cast(tipo_logradouro as string) as tipo_logradouro,
            safe_cast(tratamento_agua as string) as tratamento_agua,
            safe_cast(em_situacao_de_rua as bool) as em_situacao_de_rua,
            safe_cast(frequenta_escola as string) as frequenta_escola, -- nao sei tipo
            split(regexp_replace(meios_transporte,r'[\[|\]]',''),',') as meios_transporte,
            safe_cast(situacao_usuario as string) as situacao_usuario, -- nao sei tipo
            split(regexp_replace(doencas_condicoes,r'[\[|\]]',''),',') as doencas_condicoes,
            nullif({{clean_name_string('estado_nascimento')}},'') as estado_nascimento, 
            nullif({{clean_name_string('estado_residencia')}},'') as estado_residencia,
            safe_cast(identidade_genero as string) as identidade_genero,
            split(regexp_replace(meios_comunicacao,r'[\[|\]]',''),',') as meios_comunicacao,
            safe_cast(orientacao_sexual as string) as orientacao_sexual, -- nao sei o tipo
            safe_cast(possui_filtro_agua as bool) as possui_filtro_agua,
            safe_cast(possui_plano_saude as bool) as possui_plano_saude,
            safe_cast(situacao_familiar as string) as situacao_familiar,
            safe_cast(territorio_social as bool) as territorio_social,
            safe_cast(abastecimento_agua as string) as abastecimento_agua,
            safe_cast(animais_no_domicilio as bool) as animais_no_domicilio,
            safe_cast(cadastro_permanente as bool) as cadastro_permanente,
            safe_cast(familia_localizacao as string) as familia_localizacao,
            split(regexp_replace(em_caso_doenca_procura,r'[\[|\]]',''),',') as em_caso_doenca_procura,
            nullif(municipio_nascimento,'-1') as municipio_nascimento,
            regexp_extract(municipio_residencia,r'\[IBGE: ([0-9]{1,9})\]') as municipio_residencia,
            safe_cast(responsavel_familiar as bool) as responsavel_familiar,
            safe_cast(esgotamento_sanitario as string) as esgotamento_sanitario,
            safe_cast(situacao_moradia_posse as string) as situacao_moradia_posse,
            safe_cast(situacao_profissional as string) as situacao_profissional,
            safe_cast(vulnerabilidade_social as bool) as vulnerabilidade_social,
            safe_cast(familia_beneficiaria_cfc as bool) as familia_beneficiaria_cfc,
            safe_cast(data_atualizacao_cadastro as date) as data_atualizacao_cadastro,
            safe_cast(participa_grupo_comunitario as bool) as participa_grupo_comunitario,
            safe_cast(relacao_responsavel_familiar as string) as relacao_responsavel_familiar,
            safe_cast(membro_comunidade_tradicional as bool) as membro_comunidade_tradicional,
            timestamp_sub(timestamp(data_atualizacao_vinculo_equipe, "Brazil/East"),interval 2 hour) as data_atualizacao_vinculo_equipe,
            safe_cast(familia_beneficiaria_auxilio_brasil as bool) as familia_beneficiaria_auxilio_brasil,
            safe_cast(crianca_matriculada_creche_pre_escola as bool) as crianca_matriculada_creche_pre_escola,
            updated_at,
            loaded_at

        from dados_ficha_a
    )

select *
from ficha_a_padronizada
