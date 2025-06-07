{{
    config(
        alias="criancas_menores_5_anos",
        materialized="table",
        tags = ['subpav', 'criancas']
    )
}}

with
    source as (
        select 
            *
        from {{ source("brutos_informes_vitacare_staging", "criancas_menores_5_anos") }}
    ),

    sem_duplicatas as (
        select *
        from source
        qualify row_number() over (partition by _source_file, indice order by _loaded_at desc) = 1 
    ),

    extrair_informacoes as (
        select
            {{extract_competencia_from_path('_source_file')}} as competencia,
            {{process_null('ap')}} as ap,
            {{process_null('n_cnes_unidade')}} as cnes_unidade,
            {{process_null('nome_unidade_de_saude')}} as nome_unidade,
            {{process_null('nome_equipe_de_saude')}} as nome_equipe,
            {{process_null('codigo_da_equipe_de_saude')}} as codigo_equipe_saude,
            {{process_null('codigo_ine_equipe_de_saude')}} as codigo_ine_equipe,
            {{process_null('codigo_microarea')}} as codigo_microarea,
            {{process_null('n_dnv')}} as n_dnv,
            {{process_null('n_cpf')}} as cpf,
            {{validate_cpf('n_cpf')}} as cpf_valido,
            {{process_null('n_cns_da_crianca_cadastrada')}} as cns_pessoa_cadastrada,
            {{process_null('sexo_da_crianca_cadastrada')}} as sexo,
            {{process_null('nome_da_crianca_cadastrada')}} as nome_pessoa_cadastrada,
            {{process_null('raca_da_crianca_cadastrada')}} as raca,
            {{process_null('nome_da_mae_pessoa_cadastrada')}} as nome_mae_pessoa_cadastrada,
            {{parse_date('data_de_nascimento')}} as data_nascimento,
            {{process_null('n_do_prontuario')}} as n_prontuario,
            {{parse_date('data_da_primeira_consulta_medica_ate_28_dias')}} as data_primeira_consulta_medica_28_dias,
            {{parse_date('data_da_primeira_consulta_medica')}} as data_primeira_consulta_medica,
            {{parse_date('data_da_ultima_consulta_medica')}} as data_ultima_consulta_medica,
            {{process_null('n_de_consultas_medicas')}} as n_consultas_medicas,
            {{parse_date('data_da_primeira_consulta_enf_ate_28_dias')}} as data_primeira_consulta_enf_28_dias,
            {{parse_date('data_da_primeira_consulta_enf')}} as data_primeira_consulta_enf,
            {{parse_date('data_da_ultima_consulta_enf')}} as data_ultima_consulta_enf,
            {{process_null('n_de_consultas_enfermagem')}} as n_consultas_enfermagem,
            {{parse_date('data_da_primeira_visita_acs')}} as data_primeira_visita_acs,
            {{parse_date('data_da_ultima_visita_acs')}} as data_ultima_visita_acs,
            {{process_null('n_visitas_acs')}} as n_visitas_acs,
            {{process_null('aleitamento')}} as aleitamento,
            {{process_null('n_registro_avaliacao_desenvolvimento_infantil_denver')}} as n_avaliacao_desenvolvimento_denver,
            {{parse_date('data_orientacao_equipe_esb')}} as data_orientacao_equipe_esb,
            {{parse_date('data_coleta_de_sangue_p__triagem_neonatal_02_01_02_005_0')}} as data_coleta_teste_pezinho,
            {{process_null('resultado_teste_do_pezinho')}} as resultado_teste_pezinho,
            {{parse_date('data_registro_resultado_teste_do_pezinho')}} as data_resultado_teste_pezinho,
            {{parse_date('data_realizacao_reflexo_vermelho')}} as data_reflexo_vermelho,
            {{process_null('resultado_reflexo_vermelho')}} as resultado_reflexo_vermelho,
            {{parse_date('data_encaminhamento_teste_da_orelhinha')}} as data_encaminhamento_teste_orelhinha,
            {{parse_date('data_realizacao_teste_da_orelhinha')}} as data_realizacao_teste_orelhinha,
            {{process_null('resultado_teste_da_orelhinha')}} as resultado_teste_orelhinha,
            {{process_null('calendario_vacinal_atualizado')}} as calendario_vacinal_atualizado,
            {{parse_date('data_1d_penta')}} as data_1d_penta,
            {{parse_date('data_2d_penta')}} as data_2d_penta,
            {{parse_date('data_3d_penta')}} as data_3d_penta,
            {{process_null('peso')}} as peso,
            {{process_null('altura')}} as altura,
            {{parse_date('data_registro_antopometrico')}} as data_registro_antropometrico,
            {{process_null('perimetro_cefalico_em_centimetro')}} as perimetro_cefalico_centimetro,
            {{parse_date('data_registro_perimetro_cefalico')}} as data_registro_perimetro_cefalico,
            {{process_null('distancia_esq')}} as distancia_esq,
            {{process_null('tipo_optotipos_esq')}} as tipo_optotipos_esq,
            {{process_null('sem_correcao_esq')}} as sem_correcao_esq,
            {{process_null('com_correcao_esq')}} as com_correcao_esq,
            {{process_null('distancia_dir')}} as distancia_dir,
            {{process_null('tipo_optotipos_dir')}} as tipo_optotipos_dir,
            {{process_null('sem_correcao_dir')}} as sem_correcao_dir,
            {{process_null('com_correcao_dir')}} as com_correcao_dir,
            {{parse_date('data_consulta_snellen')}} as data_consulta_snellen,
            struct(
                _source_file as arquivo_fonte,
                safe_cast(REGEXP_EXTRACT(_source_file, r'/(\d{4}-\d{2}-\d{2})/') as timestamp) as criado_em,
                safe_cast(_extracted_at as timestamp) as extraido_em,
                safe_cast(_loaded_at as timestamp) as carregado_em
            ) as metadados,
            {{ process_null('ano_particao') }} as ano_particao,
            {{ process_null('mes_particao') }} as mes_particao,
            {{ parse_date('data_particao') }} as data_particao
        from sem_duplicatas
    )
select * from extrair_informacoes
