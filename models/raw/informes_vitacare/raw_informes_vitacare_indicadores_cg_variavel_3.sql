{{
    config(
        alias="indicadores_cg_variavel_3",
        materialized="table",
        tags = ['subpav']
    )
}}

with
    source as (
        select 
            *
        from {{ source("brutos_informes_vitacare_staging", "indicadores_cg_variavel_3") }}
    ),

    sem_duplicatas as (
        select *
        from source
        qualify row_number() over (partition by _source_file, indice order by _loaded_at desc) = 1 
    ),

    extrair_informacoes as (
        select
            {{extract_competencia_from_path('_source_file')}} as competencia,
            {{process_null('indicador')}} as indicador,
            {{process_null('ap')}} as ap,
            {{process_null('cnes')}} as cnes,
            {{process_null('unidade')}} as unidade,
            {{process_null('ine')}} as ine,
            {{process_null('equipe')}} as equipe,
            {{process_null('a')}} as a,
            {{process_null('b')}} as b,
            {{process_null('c')}} as c,
            {{process_null('d')}} as d,
            {{process_null('e')}} as e,
            {{process_null('f')}} as f,
            {{process_null('g')}} as g,
            {{process_null('h')}} as h,
            {{process_null('i')}} as i,
            {{process_null('total')}} as total,
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
