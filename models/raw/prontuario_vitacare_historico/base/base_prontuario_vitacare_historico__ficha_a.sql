{{
    config(
        schema="brutos_prontuario_vitacare_historico_staging",
        alias="_base_ficha_a_historico",
        materialized="table",
    )
}}

with

    source as (
        select
            -- PK
            concat(
                nullif({{ remove_double_quotes('id_cnes') }}, ''),
                '.',
                nullif({{ clean_numeric_string(remove_double_quotes("ut_id")) }}, '')
            ) as id, 
            *
        from {{ source("brutos_prontuario_vitacare_historico_staging", "pacientes") }}
    ),

    dados_ficha_a as (
        select
            -- PK
            cast(id as string) as id,
            
            safe_cast(nullif({{ remove_double_quotes('cpf') }}, '') as string) as cpf,
            {{ clean_numeric_string(remove_double_quotes('ut_id')) }} as id_paciente,
            {{ remove_double_quotes('npront') }} as numero_prontuario,
            {{ remove_double_quotes('cnes') }} as unidade_cadastro,
            nullif({{ remove_double_quotes('ap') }},'') as ap_cadastro,

            {{ process_null(remove_double_quotes('nome')) }} as nome,
            {{ process_null(remove_double_quotes('sexo')) }} as sexo,
            {{ process_null(remove_double_quotes('obito')) }} as obito,
            {{ process_null(remove_double_quotes('bairro')) }} as bairro,
            {{ process_null(remove_double_quotes('comodos')) }} as comodos,
            {{ process_null(remove_double_quotes('nomemae')) }} as nome_mae,
            {{ process_null(remove_double_quotes('nomepai')) }} as nome_pai,
            {{ process_null(remove_double_quotes('racacor')) }} as raca_cor,
            {{ process_null(remove_double_quotes('ocupacao')) }} as ocupacao,
            {{ process_null(remove_double_quotes('religiao')) }} as religiao,
            {{ process_null(remove_double_quotes('telefone')) }} as telefone,
            {{ process_null(remove_double_quotes('ineequipe')) }} as ine_equipe,
            {{ process_null(remove_double_quotes('microarea')) }} as microarea,
            {{ process_null(remove_double_quotes('logradouro')) }} as logradouro,
            {{ process_null(remove_double_quotes('nomesocial')) }} as nome_social,
            {{ process_null(remove_double_quotes('destinolixo')) }} as destino_lixo,
            {{ process_null(remove_double_quotes('luzeletrica')) }} as luz_eletrica,
            {{ process_null(remove_double_quotes('codigoequipe')) }} as codigo_equipe,
            {{ process_null(remove_double_quotes('datacadastro')) }} as data_cadastro,
            {{ process_null(remove_double_quotes('escolaridade')) }} as escolaridade,
            {{ process_null(remove_double_quotes('tempomoradia')) }} as tempo_moradia,
            {{ process_null(remove_double_quotes('nacionalidade')) }} as nacionalidade,
            {{ process_null(remove_double_quotes('rendafamiliar')) }} as renda_familiar,
            {{ process_null(remove_double_quotes('tipodomicilio')) }} as tipo_domicilio,
            {{ process_null(remove_double_quotes('dta_nasc')) }} as data_nascimento,
            {{ process_null(remove_double_quotes('paisnascimento')) }} as pais_nascimento,
            {{ process_null(remove_double_quotes('tipologradouro')) }} as tipo_logradouro,
            {{ process_null(remove_double_quotes('tratamentoagua')) }} as tratamento_agua,
            {{ process_null(remove_double_quotes('emsituacaoderua')) }} as em_situacao_de_rua,
            {{ process_null(remove_double_quotes('frequentaescola')) }} as frequenta_escola,
            {{ process_null(remove_double_quotes('meiostransporte')) }} as meios_transporte,
            {{ process_null(remove_double_quotes('situacaousuario')) }} as situacao_usuario,
            {{ process_null(remove_double_quotes('doencascondicoes')) }} as doencas_condicoes,
            {{ process_null(remove_double_quotes('estadonascimento')) }} as estado_nascimento,
            {{ process_null(remove_double_quotes('estadoresidencia')) }} as estado_residencia,
            {{ process_null(remove_double_quotes('identidadegenero')) }} as identidade_genero,
            {{ process_null(remove_double_quotes('meioscomunicacao')) }} as meios_comunicacao,
            {{ process_null(remove_double_quotes('orientacaosexual')) }} as orientacao_sexual,
            {{ process_null(remove_double_quotes('possuifiltroagua')) }} as possui_filtro_agua,
            {{ process_null(remove_double_quotes('possuiplanosaude')) }} as possui_plano_saude,
            {{ process_null(remove_double_quotes('situacaofamiliar')) }} as situacao_familiar,
            {{ process_null(remove_double_quotes('territoriosocial')) }} as territorio_social,
            {{ process_null(remove_double_quotes('abastecimentoagua')) }} as abastecimento_agua,
            {{ process_null(remove_double_quotes('animaisnodomicilio')) }} as animais_no_domicilio,
            {{ process_null(remove_double_quotes('cadastropermanente')) }} as cadastro_permanente,
            {{ process_null(remove_double_quotes('familialocalizacao')) }} as familia_localizacao,
            {{ process_null(remove_double_quotes('emcasodoencaprocura')) }} as em_caso_doenca_procura,
            {{ process_null(remove_double_quotes('municipionascimento')) }} as municipio_nascimento,
            {{ process_null(remove_double_quotes('municipioresidencia')) }} as municipio_residencia,
            {{ process_null(remove_double_quotes('responsavelfamiliar')) }} as responsavel_familiar,
            {{ process_null(remove_double_quotes('esgotamentosanitario')) }} as esgotamento_sanitario,
            {{ process_null(remove_double_quotes('situacaomoradiaposse')) }} as situacao_moradia_posse,
            {{ process_null(remove_double_quotes('situacaoprofissional')) }} as situacao_profissional,
            {{ process_null(remove_double_quotes('vulnerabilidadesocial')) }} as vulnerabilidade_social,
            {{ process_null(remove_double_quotes('familiabeneficiariacfc')) }} as familia_beneficiaria_cfc,
            {{ process_null(remove_double_quotes('dataatualizacaocadastro')) }} as data_atualizacao_cadastro,
            {{ process_null(remove_double_quotes('participagrupocomunitario')) }} as participa_grupo_comunitario,
            {{ process_null(remove_double_quotes('relacaoresponsavelfamiliar')) }} as relacao_responsavel_familiar,
            {{ process_null(remove_double_quotes('membrocomunidadetradicional')) }} as membro_comunidade_tradicional,
            {{ process_null(remove_double_quotes('dataatualizacaovinculoequipe')) }} as data_atualizacao_vinculo_equipe,
            {{ process_null(remove_double_quotes('familiabeneficiariaauxiliobrasil')) }} as familia_beneficiaria_auxilio_brasil,
            {{ process_null(remove_double_quotes('criancamatriculadacrechepreescola')) }} as crianca_matriculada_creche_pre_escola,

            updated_at as updated_at,
            extracted_at as loaded_at
        from source
    )

select *
from dados_ficha_a