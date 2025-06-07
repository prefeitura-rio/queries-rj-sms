{{
    config(
        alias="estado_nutricional_sisvan",
        materialized="table",
        tags = ['subpav', 'estado_nutricional','sisvan']
    )
}}

with
    source as (
        select 
            *
        from {{ source("brutos_informes_vitacare_staging", "estado_nutricional_sisvan") }}
    ),

    sem_duplicatas as (
        select *
        from source
        qualify row_number() over (partition by _source_file, indice order by _loaded_at desc) = 1 
    ),

    extrair_informacoes as (
        select
            {{extract_competencia_from_path('_source_file')}} as competencia,
            {{process_null('cod_seg')}} as ap,
            {{process_null('cnes')}} as cnes_unidade,
            {{process_null('nome_ub')}} as nome_unidade,
            {{process_null('cod_area')}} as codigo_equipe_saude,
            {{process_null('cod_microa')}} as codigo_microarea,
            {{process_null('nome')}} as nome_pessoa_cadastrada,
            {{parse_date('nascimento')}} as data_nascimento,
            {{process_null('sexo')}} as sexo,
            {{process_null('nome_resp')}} as nome_responsavel_pessoa_cadastrada,
            {{process_null('bairro')}} as bairro_moradia,
            {{process_null('cep')}} as cep_logradouro,
            {{parse_date('dt_atend')}} as data_atendimento,
            {{process_null('peso')}} as peso,
            {{process_null('altura')}} as altura,
            {{process_null('ig')}} as idade_gestacional,
            {{process_null('estnut_criancas_est')}} as estnut_crianca_estatura,
            {{process_null('estnut_criancas_peso')}} as estnut_crianca_peso,
            {{process_null('estnut_criancas_imc')}} as estnut_crianca_imc,
            {{process_null('estnut_adolescentes_imc')}} as estnut_adolescentes_imc,
            {{process_null('estnut_adolescentes_est')}} as estnut_adolescentes_estatura,
            {{process_null('estnut_adultos')}} as estnut_adulto_imc,
            {{process_null('estnut_idosos')}} as estnut_idosos_imc,
            {{process_null('estnut_gestantes')}} as estnut_gestante_imc,
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