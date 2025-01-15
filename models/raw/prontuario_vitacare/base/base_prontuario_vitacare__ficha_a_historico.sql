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
            -- PK
            concat(
                nullif(id_cnes, ''),
                '.',
                nullif({{ clean_numeric_string("ut_id") }}, '')
            ) as id, 
            * except (backup_created_at),  -- TODO: change data type to string to correct load the column in bigquery
        from {{ source("brutos_prontuario_vitacare_staging", "pacientes_historico") }}
    ),

    dados_ficha_a as (
        select
            safe_cast(nullif(cpf, '') as string) as cpf,
            ut_id as id_paciente,

            cnes as unidade_cadastro,
            ap as ap_cadastro,

            {{ process_null('nome') }} as nome,
            {{ process_null('sexo') }} as sexo,
            {{ process_null('obito') }} as obito,
            {{ process_null('bairro') }} as bairro,
            {{ process_null('comodos') }} as comodos,
            {{ process_null('nomemae') }} as nome_mae,
            {{ process_null('nomepai') }} as nome_pai,
            {{ process_null('racacor') }} as raca_cor,
            {{ process_null('ocupacao') }} as ocupacao,
            {{ process_null('religiao') }} as religiao,
            {{ process_null('telefone') }} as telefone,
            {{ process_null('ineequipe') }} as ine_equipe,
            {{ process_null('microarea') }} as microarea,
            {{ process_null('logradouro') }} as logradouro,
            {{ process_null('nomesocial') }} as nome_social,
            {{ process_null('destinolixo') }} as destino_lixo,
            {{ process_null('luzeletrica') }} as luz_eletrica,
            {{ process_null('codigoequipe') }} as codigo_equipe,
            {{ process_null('datacadastro') }} as data_cadastro,
            {{ process_null('escolaridade') }} as escolaridade,
            {{ process_null('tempomoradia') }} as tempo_moradia,
            {{ process_null('nacionalidade') }} as nacionalidade,
            {{ process_null('rendafamiliar') }} as renda_familiar,
            {{ process_null('tipodomicilio') }} as tipo_domicilio,
            {{ process_null('dta_nasc') }} as data_nascimento,
            {{ process_null('paisnascimento') }} as pais_nascimento,
            {{ process_null('tipologradouro') }} as tipo_logradouro,
            {{ process_null('tratamentoagua') }} as tratamento_agua,
            {{ process_null('emsituacaoderua') }} as em_situacao_de_rua,
            {{ process_null('frequentaescola') }} as frequenta_escola,
            {{ process_null('meiostransporte') }} as meios_transporte,
            {{ process_null('situacaousuario') }} as situacao_usuario,
            {{ process_null('doencascondicoes') }} as doencas_condicoes,
            {{ process_null('estadonascimento') }} as estado_nascimento,
            {{ process_null('estadoresidencia') }} as estado_residencia,
            {{ process_null('identidadegenero') }} as identidade_genero,
            {{ process_null('meioscomunicacao') }} as meios_comunicacao,
            {{ process_null('orientacaosexual') }} as orientacao_sexual,
            {{ process_null('possuifiltroagua') }} as possui_filtro_agua,
            {{ process_null('possuiplanosaude') }} as possui_plano_saude,
            {{ process_null('situacaofamiliar') }} as situacao_familiar,
            {{ process_null('territoriosocial') }} as territorio_social,
            {{ process_null('abastecimentoagua') }} as abastecimento_agua,
            {{ process_null('animaisnodomicilio') }} as animais_no_domicilio,
            {{ process_null('cadastropermanente') }} as cadastro_permanente,
            {{ process_null('familialocalizacao') }} as familia_localizacao,
            {{ process_null('emcasodoencaprocura') }} as em_caso_doenca_procura,
            {{ process_null('municipionascimento') }} as municipio_nascimento,
            {{ process_null('municipioresidencia') }} as municipio_residencia,
            {{ process_null('responsavelfamiliar') }} as responsavel_familiar,
            {{ process_null('esgotamentosanitario') }} as esgotamento_sanitario,
            {{ process_null('situacaomoradiaposse') }} as situacao_moradia_posse,
            {{ process_null('situacaoprofissional') }} as situacao_profissional,
            {{ process_null('vulnerabilidadesocial') }} as vulnerabilidade_social,
            {{ process_null('familiabeneficiariacfc') }} as familia_beneficiaria_cfc,
            {{ process_null('dataatualizacaocadastro') }} as data_atualizacao_cadastro,
            {{ process_null('participagrupocomunitario') }} as participa_grupo_comunitario,
            {{ process_null('relacaoresponsavelfamiliar') }} as relacao_responsavel_familiar,
            {{ process_null('membrocomunidadetradicional') }} as membro_comunidade_tradicional,
            {{ process_null('dataatualizacaovinculoequipe') }} as data_atualizacao_vinculo_equipe,
            {{ process_null('familiabeneficiariaauxiliobrasil') }} as familia_beneficiaria_auxilio_brasil,
            {{ process_null('criancamatriculadacrechepreescola') }} as crianca_matriculada_creche_pre_escola,

            updated_at as updated_at,
            datalake_imported_at as loaded_at
        from source
    ),
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
