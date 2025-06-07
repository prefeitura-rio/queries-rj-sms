{{
    config(
        alias="ficha_c_v2",
        materialized="table",
        tags = ['subpav', 'ficha_c']
    )
}}

with
    source as (
        select 
            *
        from {{ source("brutos_informes_vitacare_staging", "ficha_c_v2") }}
    ),

    sem_duplicatas as (
        select *
        from source
        qualify row_number() over (partition by _source_file, indice order by _loaded_at desc) = 1 
    ),

    extrair_informacoes as (
        select
            {{extract_competencia_from_path('_source_file')}} as competencia,
            {{ process_null('ap') }} as ap,
            {{ process_null('cnes_da_unidade') }} as cnes_unidade,
            {{ process_null('nome_da_unidade') }} as nome_unidade,
            {{ process_null('codigo_da_equipe') }} as codigo_equipe_saude,
            {{ process_null('codigo_ine_da_equipe') }} as codigo_ine_equipe,
            {{ process_null('nome_da_equipe') }} as nome_equipe,
            {{ process_null('numero_da_microarea_cnes') }} as codigo_microarea,
            {{ process_null('nome_do_acs') }} as nome_acs,
            {{ process_null('numero_cns_do_acs') }} as cns_acs,
            {{ process_null('nome_do_paciente_cadastrado') }} as nome_pessoa_cadastrada,
            {{ process_null('nome_social_do_paciente_cadastrado') }} as nome_social_pessoa_cadastrada,
            {{ parse_date('data_de_cadastro') }} as data_cadastro,
            {{ process_null('cpf_do_paciente') }} as cpf,
            {{ validate_cpf('cpf_do_paciente') }} as cpf_valido,
            {{ process_null('dnv_do_paciente') }} as n_dnv,
            {{ process_null('cns_do_paciente') }} as cns_pessoa_cadastrada,
            {{ process_null('nome_da_mae_do_paciente') }} as nome_mae_pessoa_cadastrada,
            {{ process_null('numero_da_familia') }} as n_familia,
            {{ process_null('numero_do_prontuario') }} as n_prontuario,
            {{ process_null('sexo') }} as sexo,
            {{ process_null('raca') }} as raca_cor,
            {{ parse_date('data_de_nascimento') }} as data_nascimento,
            {{ process_null('idade') }} as idade,
            {{ process_null('situacao_do_usuario') }} as situacao_usuario,
            {{ process_null('obito') }} as obito,
            {{ process_null('logradouro_residencia') }} as tipo_logradouro,
            {{ process_null('logradouro_endereco') }} as logradouro_endereco,
            {{ process_null('logradouro_numero') }} as logradouro_numero,
            {{ process_null('cep_logradouro') }} as cep_logradouro,
            {{ process_null('bairro_de_moradia') }} as bairro_moradia,
            {{ process_null('telefone_ou_celular_de_contato') }} as telefone_contato,
            {{ process_null('escola') }} as escola,
            {{ process_null('perfil_bpc') }} as perfil_bpc,
            {{ process_null('crianca_matriculada_pre_escola') }} as crianca_matriculada_pre_escola,
            {{ process_null('afastada_creche_ou_pre_escola') }} as afastada_creche_pre_escola,
            {{ process_null('afastada_da_escola_por_motivo_saude') }} as afastada_da_escola_por_motivo_saude,
            {{ process_null('crianca_faltou') }} as crianca_faltou,
            {{ process_null('atividade_contraturno') }} as atividade_contraturno,
            {{ process_null('registros_importantes') }} as registros_importantes,
            {{ process_null('onde_dorme') }} as onde_dorme,
            {{ process_null('comparece_consultas') }} as comparece_consultas,
            {{ process_null('vacinas_em_dia_sim_nao') }} as vacinas_em_dia,
            {{ process_null('primeira_cons_7_dias') }} as primeira_cons_7_dias,
            {{ process_null('estado_nutricional') }} as estado_nutricional,
            {{ process_null('tipo_de_aleitamento') }} as tipo_de_aleitamento,
            {{ process_null('atraso_desenvolvimento') }} as atraso_desenvolvimento,
            {{ process_null('sinais_risco') }} as sinais_risco,
            {{ process_null('diarreia') }} as diarreia,
            {{ process_null('infeccao_respirat_aguda') }} as infeccao_respirat_aguda,
            {{ process_null('tipo_de_parto') }} as tipo_de_parto,
            {{ process_null('estatura_ao_nascer') }} as estatura_ao_nascer,
            {{ process_null('peso_ao_nascer') }} as peso_ao_nascer,
            {{ process_null('altura') }} as altura,
            {{ process_null('peso') }} as peso,
            {{ process_null('per_cefalico') }} as per_cefalico,
            {{ process_null('apgar_5') }} as apgar_5,
            {{ parse_date('data_acs_visita') }} as data_acs_visita,
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