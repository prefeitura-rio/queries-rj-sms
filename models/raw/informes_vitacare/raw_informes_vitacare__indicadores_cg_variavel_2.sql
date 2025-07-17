{{
    config(
        alias="indicadores_cg_variavel_2",
        materialized="table",
        tags = ['subpav', 'indicadores']
    )
}}

with
    source as (
        select 
            *
        from {{ source("brutos_informes_vitacare_staging", "indicadores_cg_variavel_2") }}
    ),

    sem_duplicatas as (
        select *
        from source
        qualify row_number() over (partition by _source_file, indice order by _loaded_at desc) = 1 
    ),

    extrair_informacoes as (
        select
            {{extract_competencia_from_path('_source_file')}} as competencia,
            {{normalize_null('indicador')}} as indicador,
            {{normalize_null('grandeza')}} as grandeza,
            {{normalize_null('ap')}} as ap,
            {{normalize_null('cnes')}} as cnes,
            {{normalize_null('unidade')}} as unidade,
            {{normalize_null('ine')}} as ine,
            {{normalize_null('equipe')}} as equipe,
            {{normalize_null('num')}} as numerador,
            {{normalize_null('den')}} as denominador,
            struct(
                _source_file as arquivo_fonte,
                safe_cast(REGEXP_EXTRACT(_source_file, r'/(\d{4}-\d{2}-\d{2})/') as timestamp) as criado_em,
                safe_cast(_extracted_at as timestamp) as extraido_em,
                safe_cast(_loaded_at as timestamp) as carregado_em
            ) as metadados,
            {{ normalize_null('ano_particao') }} as ano_particao,
            {{ normalize_null('mes_particao') }} as mes_particao,
            {{ parse_date('data_particao') }} as data_particao
        from sem_duplicatas
    )
select * from extrair_informacoes