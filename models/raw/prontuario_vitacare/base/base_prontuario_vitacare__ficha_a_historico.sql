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
            npront as numero_prontuario,
            
            cnes as unidade_cadastro,
            nullif(ap,'') as ap_cadastro,

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
    )

select *
from dados_ficha_a
