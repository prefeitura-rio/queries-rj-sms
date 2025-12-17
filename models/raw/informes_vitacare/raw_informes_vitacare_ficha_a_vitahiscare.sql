{{
    config(
        alias="ficha_a_vitahiscare",
        materialized="table",
    )
}}

with
    source as (
        select
            *
        from {{ source("brutos_informes_vitacare_staging", "ficha_a_vitahiscare") }}
    ),

    sem_duplicatas as (
        select *
        from source
        qualify row_number() over (
            partition by _source_file, indice
            order by _loaded_at desc
        ) = 1
    ),

    extrair_informacoes as (
        select

            {{ process_null("ap") }} as ap,
            {{ process_null("numero_cnes_unidade") }} as numero_cnes_unidade,
            {{ process_null("nome_unidade_de_saude") }} as nome_unidade_de_saude,
            {{ process_null("nome_equipe_de_saude") }} as nome_equipe_de_saude,
            {{ process_null("codigo_da_equipe_de_saude") }} as codigo_da_equipe_de_saude,
            {{ process_null("codigo_ine_equipe_de_saude") }} as codigo_ine_equipe_de_saude,
            {{ process_null("codigo_microarea") }} as codigo_microarea,
            {{ process_null("n_dnv") }} as n_dnv,
            {{ process_null("n_cpf") }} as n_cpf,
            {{ process_null("nome_acs") }} as nome_acs,
            {{ process_null("n_cns_da_pessoa_cadastrada") }} as n_cns_da_pessoa_cadastrada,
            {{ process_null("nome_da_pessoa_cadastrada") }} as nome_da_pessoa_cadastrada,
            {{ process_null("nome_da_mae_pessoa_cadastrada") }} as nome_da_mae_pessoa_cadastrada,
            {{ process_null("n_da_familia") }} as n_da_familia,
            {{ process_null("n_do_prontuario") }} as n_do_prontuario,
            {{ parse_date(process_null("data_cadastro")) }} as data_cadastro,
            {{ parse_date(process_null("data_ultima_atualizacao_do_cadastro")) }} as data_ultima_atualizacao_cadastro,
            {{ process_null("situacao_usuario") }} as situacao_usuario,
            {{ process_null("obito") }} as obito,
            {{ process_null("sexo") }} as sexo,
            {{ process_null("raca_cor") }} as raca_cor,
            {{ parse_date(process_null("data_de_nascimento")) }} as data_nascimento,
            {{ process_null("alcoolismo") }} as alcoolismo,
            {{ process_null("doenca_de_chagas") }} as doenca_de_chagas,
            {{ process_null("deficiencia_fisica") }} as deficiencia_fisica,
            {{ process_null("doenca_mental") }} as doenca_mental,
            {{ process_null("epilepsia") }} as epilepsia,
            {{ process_null("diabetes") }} as diabetes,
            {{ process_null("gestante") }} as gestante,
            {{ process_null("hanseniase") }} as hanseniase,
            {{ process_null("hipertensao_arterial") }} as hipertensao_arterial,
            {{ process_null("malaria") }} as malaria,
            {{ process_null("tuberculose") }} as tuberculose,
            {{ process_null("tabagismo") }} as tabagismo,
            {{ process_null("frequenta_escola") }} as frequenta_escola,
            {{ process_null("tipo_de_logradouro") }} as tipo_de_logradouro,
            {{ process_null("logradouro") }} as logradouro,
            {{ process_null("cep_logradouro") }} as cep_logradouro,
            {{ process_null("bairro_de_moradia") }} as bairro_de_moradia,
            {{ process_null("comunidade_moradia") }} as comunidade_moradia,
            {{ process_null("telefone_contato") }} as telefone_contato,
            {{ process_null("email_contato") }} as email_contato,
            {{ process_null("tipo_de_domicilio") }} as tipo_de_domicilio,
            {{ process_null("existencia_de_energia_eletrica") }} as existencia_de_energia_eletrica,
            {{ process_null("destino_do_lixo") }} as destino_do_lixo,
            {{ process_null("abastecimento_de_agua") }} as abastecimento_de_agua,
            {{ process_null("tratamento_de_agua") }} as tratamento_de_agua,
            {{ process_null("esgotamento_sanitario") }} as esgotamento_sanitario,
            {{ process_null("possui_plano_de_saude") }} as possui_plano_de_saude,
            {{ process_null("em_caso_de_doenca_procura") }} as em_caso_de_doenca_procura,
            {{ process_null("cultiva_plantas_medicinais") }} as cultiva_plantas_medicinais,
            {{ process_null("usa_plantas_medicinais") }} as usa_plantas_medicinais,
            {{ process_null("tem_animais_no_domicilio") }} as tem_animais_no_domicilio,
            {{ process_null("qualis") }} as qualis,
            {{ process_null("ja_sofreu_ataque_de_morcego") }} as ja_sofreu_ataque_de_morcego,
            {{ process_null("meios_de_comunicacao_que_utiliza") }} as meios_de_comunicacao_que_utiliza,
            {{ process_null("meios_de_transporte_que_utiliza") }} as meios_de_transporte_que_utiliza,
            {{ process_null("participacao_em_grupos_comunitarios") }} as participacao_em_grupos_comunitarios,
            {{ process_null("renda_familiar") }} as renda_familiar,
            {{ process_null("vunerabilidade_social") }} as vunerabilidade_social,
            {{ process_null("familia_beneficiaria_bolsa_familia") }} as familia_beneficiaria_bolsa_familia,
            {{ process_null("familia_beneficiaria_cfc") }} as familia_beneficiaria_cfc,
            {{ process_null("nis") }} as nis,

            {{ process_null("n_de_consultas_2018") }} as n_de_consultas_2018,
            {{ process_null("n_de_procedimentos_2018") }} as n_de_procedimentos_2018,
            {{ process_null("n_de_consultas_2017") }} as n_de_consultas_2017,
            {{ process_null("n_de_procedimentos_2017") }} as n_de_procedimentos_2017,
            {{ process_null("n_de_consultas_2016") }} as n_de_consultas_2016,
            {{ process_null("n_de_procedimentos_2016") }} as n_de_procedimentos_2016,
            {{ process_null("n_de_consultas_2015") }} as n_de_consultas_2015,
            {{ process_null("n_de_procedimentos_2015") }} as n_de_procedimentos_2015,
            {{ process_null("n_de_consultas_2014") }} as n_de_consultas_2014,
            {{ process_null("n_de_procedimentos_2014") }} as n_de_procedimentos_2014,
            {{ process_null("n_de_consultas_2013") }} as n_de_consultas_2013,
            {{ process_null("n_de_procedimentos_2013") }} as n_de_procedimentos_2013,
            {{ process_null("n_de_consultas_2012") }} as n_de_consultas_2012,
            {{ process_null("n_de_procedimentos_2012") }} as n_de_procedimentos_2012,
            {{ process_null("n_de_consultas_2011") }} as n_de_consultas_2011,
            {{ process_null("n_de_procedimentos_2011") }} as n_de_procedimentos_2011,

            {{ parse_date(process_null("data_1a_consulta")) }} as data_1a_consulta,
            {{ parse_date(process_null("data_2a_consulta")) }} as data_2a_consulta,

            {{ parse_date(process_null("data_da_ultima_consulta")) }} as data_ultima_consulta,
            {{ parse_date(process_null("data_da_ultima_consulta_med_enf_propria_equipe")) }} as data_ultima_consulta_med_enf_propria_equipe,

            {{ process_null("hist_cid") }} as hist_cid,

            -- CIDs e datas de registro (01--10)
            {% for i in range(1, 11) %}
                {% set idx = "%02d" % i %}
                {{ parse_date(process_null("data_registro_cid_" ~ idx)) }} as data_registro_cid_{{ idx }},
                {{ process_null("cid_" ~ idx) }} as cid_{{ idx }}{% if not loop.last %},{% endif %}
            {% endfor %},
    
            {{ process_null("primeiro_cid_extra_por_campo") }} as primeiro_cid_extra_por_campo,
            {{ parse_date(process_null("data_registro_cid_extra_01")) }} as data_registro_cid_extra_01,

            {{ process_null("segundo_cid_extra_por_campo") }} as segundo_cid_extra_por_campo,
            {{ parse_date(process_null("data_registro_cid_extra_02")) }} as data_registro_cid_extra_02,

            {{ process_null("peso") }} as peso,
            {{ process_null("altura") }} as altura,
            {{ parse_date(process_null("data_registro_peso_altura")) }} as data_registro_peso_altura,

            {{ parse_date(process_null("data_ultima_menstruacao")) }} as data_ultima_menstruacao,

            struct(
                _source_file as arquivo_fonte,
                safe_cast(REGEXP_EXTRACT(_source_file, r'/(\d{4}-\d{2}-\d{2})/') as timestamp) as criado_em,
                safe_cast(_extracted_at as timestamp) as extraido_em,
                safe_cast(_loaded_at as timestamp) as carregado_em
            ) as metadados,
            {{ process_null("ano_particao") }} as ano_particao,
            {{ process_null("mes_particao") }} as mes_particao,
            {{ process_null("data_particao") }} as data_particao

        from sem_duplicatas
    )
select * from extrair_informacoes
