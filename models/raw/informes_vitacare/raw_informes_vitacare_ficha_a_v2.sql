{{
    config(
        alias="ficha_a_v2",
        materialized="table",
    )
}}

with
    source as (
        select 
            *
        from {{ source("brutos_informes_vitacare_staging", "ficha_a_v2") }}
    ),

    sem_duplicatas as (
        select *
        from source
        qualify row_number() over (partition by _source_file, indice order by _loaded_at desc) = 1 
    ),

    extrair_informacoes as (
        select
            {{ process_null('ap') }} as ap,
            {{ process_null('numero_cnes_unidade') }} as cnes_unidade,
            {{ process_null('nome_unidade_de_saude') }} as nome_unidade,
            {{ process_null('nome_equipe_de_saude') }} as nome_equipe,
            {{ process_null('codigo_da_equipe_de_saude') }} as codigo_equipe_saude,
            {{ process_null('codigo_ine_equipe_de_saude') }} as codigo_ine_equipe,
            {{ process_null('codigo_microarea') }} as codigo_microarea,
            {{ process_null('n_dnv') }} as n_dnv,
            {{ process_null('n_cpf') }} as cpf,
            {{ validate_cpf('n_cpf') }} as cpf_valido,
            {{ process_null('nis') }} as nis,
            {{ process_null('nome_acs') }} as nome_acs,
            {{ process_null('n_cns_da_pessoa_cadastrada') }} as cns_pessoa_cadastrada,
            {{ process_null('nome_da_pessoa_cadastrada') }} as nome_pessoa_cadastrada,
            {{ process_null('nome_social_da_pessoa_cadastrada') }} as nome_social_pessoa_cadastrada,
            {{ process_null('nome_da_mae_pessoa_cadastrada') }} as nome_mae_pessoa_cadastrada,
            {{ process_null('n_da_familia') }} as n_familia,
            {{ process_null('n_do_prontuario') }} as n_prontuario,
            cast({{ process_null('data_cadastro') }} as date format 'DD/MM/YYYY') as data_cadastro,
            cast({{ process_null('data_ultima_atualizacao_do_cadastro') }} as date format 'DD/MM/YYYY') as data_ultima_atualizacao_cadastro,
            {{ process_null('situacao_usuario') }} as situacao_usuario,
            {{ process_null('obito') }} as obito,
            {{ process_null('sexo') }} as sexo,
            {{ process_null('orientacao_sexual') }} as orientacao_sexual,
            {{ process_null('identidade_genero') }} as identidade_genero,
            {{ process_null('raca_cor') }} as raca_cor,
            cast({{ process_null('data_de_nascimento') }} as date format 'DD/MM/YYYY') as data_nascimento,
            {{ process_null('situacao_profissional') }} as situacao_profissional,
            {{ process_null('ocupacao') }} as ocupacao,
            {{ process_null('nacionalidade') }} as nacionalidade,
            {{ process_null('pais_de_nascimento') }} as pais_nascimento,
            {{ process_null('municipio_de_nascimento') }} as municipio_nascimento,
            {{ process_null('alcoolismo') }} as alcoolismo,
            {{ process_null('doenca_de_chagas') }} as doenca_chagas,
            {{ process_null('deficiencia_fisica') }} as deficiencia_fisica,
            {{ process_null('doenca_mental') }} as doenca_mental,
            {{ process_null('epilepsia') }} as epilepsia,
            {{ process_null('diabetes') }} as diabetes,
            {{ process_null('gestante') }} as gestante,
            {{ process_null('hanseniase') }} as hanseniase,
            {{ process_null('hipertensao_arterial') }} as hipertensao_arterial,
            {{ process_null('malaria') }} as malaria,
            {{ process_null('tuberculose') }} as tuberculose,
            {{ process_null('tabagismo') }} as tabagismo,
            {{ process_null('frequenta_escola') }} as frequenta_escola,
            {{ process_null('tipo_de_logradouro') }} as tipo_logradouro,
            {{ process_null('logradouro') }} as logradouro,
            {{ process_null('cep_logradouro') }} as cep_logradouro,
            {{ process_null('bairro_de_moradia') }} as bairro_moradia,
            {{ process_null('comunidade_moradia') }} as comunidade_moradia,
            {{ process_null('comunidade_tradicionais') }} as comunidade_tradicionais,
            {{ process_null('telefone_contato') }} as telefone_contato,
            {{ process_null('email_contato') }} as email_contato,
            {{ process_null('tipo_de_domicilio') }} as tipo_domicilio,
            {{ process_null('existencia_de_energia_eletrica') }} as existencia_energia_eletrica,
            {{ process_null('destino_do_lixo') }} as destino_lixo,
            {{ process_null('abastecimento_de_agua') }} as abastecimento_agua,
            {{ process_null('tratamento_de_agua') }} as tratamento_agua,
            {{ process_null('possui_filtro_agua') }} as possui_filtro_agua,
            {{ process_null('esgotamento_sanitario') }} as esgotamento_sanitario,
            {{ process_null('possui_plano_de_saude') }} as possui_plano_de_saude,
            {{ process_null('em_caso_de_doenca_procura') }} as em_caso_de_doenca_procura,
            {{ process_null('cultiva_plantas_medicinais') }} as cultiva_plantas_medicinais,
            {{ process_null('usa_plantas_medicinais') }} as usa_plantas_medicinais,
            {{ process_null('tem_animais_no_domicilio') }} as tem_animais_no_domicilio,
            {{ process_null('qualis') }} as qualis,
            {{ process_null('ja_sofreu_ataque_de_morcego') }} as ja_sofreu_ataque_de_morcego,
            {{ process_null('meios_de_comunicacao_que_utiliza') }} as meios_de_comunicacao_que_utiliza,
            {{ process_null('meios_de_transporte_que_utiliza') }} as meios_de_transporte_que_utiliza,
            {{ process_null('participacao_em_grupos_comunitarios') }} as participacao_em_grupos_comunitarios,
            {{ process_null('renda_familiar') }} as renda_familiar,
            {{ process_null('vunerabilidade_social') }} as vunerabilidade_social,
            {{ process_null('familia_beneficiaria_auxilio_brasil') }} as familia_beneficiaria_auxilio_brasil,
            {{ process_null('familia_beneficiaria_cfc') }} as familia_beneficiaria_cfc,
            {{ process_null('n_de_consultas_2023') }} as n_consultas_2023,
            {{ process_null('n_de_procedimentos_2023') }} as n_procedimentos_2023,
            {{ process_null('n_de_consultas_2022') }} as n_consultas_2022,
            {{ process_null('n_de_procedimentos_2022') }} as n_procedimentos_2022,
            {{ process_null('n_de_consultas_2021') }} as n_consultas_2021,
            {{ process_null('n_de_procedimentos_2021') }} as n_procedimentos_2021,
            {{ process_null('n_de_consultas_2020') }} as n_consultas_2020,
            {{ process_null('n_de_procedimentos_2020') }} as n_procedimentos_2020,
            {{ process_null('n_de_consultas_2019') }} as n_consultas_2019,
            {{ process_null('n_de_procedimentos_2019') }} as n_procedimentos_2019,
            {{ process_null('n_de_consultas_2018') }} as n_consultas_2018,
            {{ process_null('n_de_procedimentos_2018') }} as n_procedimentos_2018,
            case 
                when REGEXP_CONTAINS({{ process_null('data_1a_consulta') }},r'\d{4}-\d{2}-\d{2}') 
                    then cast(data_1a_consulta as date format 'YYYY-MM-DD')
                when REGEXP_CONTAINS({{ process_null('data_1a_consulta') }},r'\d{2}/\d{2}/\d{4}') 
                    then cast(data_1a_consulta as date format 'DD/MM/YYYY')
                else null
            end as data_1a_consulta, 
            case 
                when REGEXP_CONTAINS({{ process_null('data_2a_consulta') }},r'\d{4}-\d{2}-\d{2}') 
                    then cast(data_2a_consulta as date format 'YYYY-MM-DD')
                when REGEXP_CONTAINS({{ process_null('data_2a_consulta') }},r'\d{2}/\d{2}/\d{4}') 
                    then cast(data_2a_consulta as date format 'DD/MM/YYYY')
                else null
            end as data_2a_consulta, 
            case 
                when REGEXP_CONTAINS({{ process_null('data_da_ultima_consulta') }},r'\d{4}-\d{2}-\d{2}') 
                    then cast(data_da_ultima_consulta as date format 'YYYY-MM-DD')
                when REGEXP_CONTAINS({{ process_null('data_da_ultima_consulta') }},r'\d{2}/\d{2}/\d{4}') 
                    then cast(data_da_ultima_consulta as date format 'DD/MM/YYYY')
                else null
            end as data_ultima_consulta, 
            {{ process_null('data_da_ultima_consulta_med_enf_propria_equipe') }} as data_ultima_consulta_med_enf_propria_equipe,
            {{ process_null('hist_cid') }} as hist_cid,
            -- CIDs e datas de registro
            {% for i in range(1, 11) %}
                {% set idx = "%02d" % i %}
                    {{ process_null('data_registro_cid_' ~ idx) }} as data_registro_cid_{{ idx }},
                    {{ process_null('cid_' ~ idx) }} as cid_{{ idx }}{% if not loop.last %},{% endif %}
            {% endfor %},
            {{ process_null('peso') }} as peso,
            {{ process_null('altura') }} as altura,
            {{ process_null('data_registro_peso_altura') }} as data_registro_peso_altura,
            {{ process_null('data_ultima_menstruacao') }} as data_ultima_menstruacao,
            {{ process_null('paciente_temporario') }} as paciente_temporario,
            {{ process_null('paciente_situacao_rua') }} as paciente_situacao_rua,
            {{ process_null('situacao_moradia_posse') }} as situacao_moradia_posse,
            {{ process_null('nome_instituicao') }} as nome_instituicao,
            {{ process_null('instituicao_publica_privada') }} as instituicao_publica_privada,
            {{ process_null('possui_prof_saude') }} as possui_prof_saude,
            {{ process_null('tipo_unidade_permanencia') }} as tipo_unidade_permanencia,
            {{ process_null('familia_localizacao') }} as familia_localizacao,
            {{ process_null('tempo_moradia') }} as tempo_moradia,
            {{ process_null('comodos') }} as comodos,
            {{ process_null('situacao_familiar') }} as situacao_familiar,
            {{ process_null('escolaridade') }} as escolaridade,
            {{ process_null('territorio_social') }} as territorio_social,
            {{ process_null('religiao') }} as religiao,
            {{ process_null('certidao_nascimento') }} as certidao_nascimento,
            {{ process_null('crianca_mat_creche_ou_pre_escola') }} as crianca_mat_creche_ou_pre_escola,
            {{ process_null('responsavel') }} as responsavel,
            {{ process_null('relacao_parentesco_responsavel') }} as relacao_parentesco_responsavel,
            {{ process_null('fumante') }} as fumante,
            {{ process_null('fumante_tempo') }} as fumante_tempo,
            {{ process_null('fumante_quant') }} as fumante_quant,
            {{ process_null('qual_a_origem_da_alimentacao') }} as qual_a_origem_da_alimentacao,
            {{ process_null('faz_uso_de_drogas') }} as faz_uso_de_drogas,
            {{ process_null('situacao_rua_tempo_situacao') }} as situacao_rua_tempo_situacao,
            {{ process_null('quantas_vezes_se_alimenta_por_dia') }} as quantas_vezes_se_alimenta_por_dia,
            {{ process_null('informacoes_complementares') }} as informacoes_complementares,
            {{ process_null('apresenta_manchas_no_corpo') }} as apresenta_manchas_no_corpo,
            {{ process_null('faz_tratamento_em_algum_lugar') }} as faz_tratamento_em_algum_lugar,
            {{ process_null('ja_foi_internado') }} as ja_foi_internado,
            {{ process_null('local_permanencia_periodo_noturno') }} as local_permanencia_periodo_noturno,
            {{ process_null('possui_referencia_familiar') }} as possui_referencia_familiar,
            {{ process_null('recebe_algum_beneficio') }} as recebe_algum_beneficio,
            {{ process_null('usa_alguma_medicacao') }} as usa_alguma_medicacao,
            {{ process_null('visita_algum_familiar_com_frequencia') }} as visita_algum_familiar_com_frequencia,
            {{ process_null('tem_alguma_doenca') }} as tem_alguma_doenca,
            {{ process_null('apresenta_tosse') }} as apresenta_tosse,
            {{ process_null('local_permanencia_periodo_diurno') }} as local_permanencia_periodo_diurno,
            {{ process_null('local_realizacao_cadastramento') }} as local_realizacao_cadastramento,
            {{ process_null('tem_acesso_a_higiene_pessoal') }} as tem_acesso_a_higiene_pessoal,
            {{ process_null('apresenta_feridas_no_corpo') }} as apresenta_feridas_no_corpo,
            struct(
                _source_file as arquivo_fonte,
                safe_cast(REGEXP_EXTRACT(_source_file, r'/(\d{4}-\d{2}-\d{2})/') as timestamp) as criado_em,
                safe_cast(_extracted_at as timestamp) as extraido_em,
                safe_cast(_loaded_at as timestamp) as carregado_em
            ) as metadados,
            {{ process_null('ano_particao') }} as ano_particao,
            {{ process_null('mes_particao') }} as mes_particao,
            {{ process_null('data_particao') }} as data_particao
        from sem_duplicatas
    )
select * from extrair_informacoes
