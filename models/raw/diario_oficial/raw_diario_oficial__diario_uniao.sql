{{
    config(
        alias="diario_uniao",
        materialized="table"
    )
}}

with 
    source as (
        select *
        from {{ source("brutos_diario_oficial_staging", "diarios_uniao") }}
    ),

    diario_padronizado as (
        select
            {{ process_null('title') }} as titulo,
            {{ process_null('info') }} as info,
            parse_date('%d/%m/%Y', published_at) as data_publicacao,
            {{ process_null('edition') }} as edicao,
            {{ process_null('section') }} as secao, 
            {{ process_null('page') }} as pagina,
            {{ process_null('agency') }} as organizacao_principal,
            trim({{ process_null('text') }}) as texto,
            {{ process_null('html') }} as html, 
            {{ process_null('url') }} as link,
            safe_cast(_extracted_at as timestamp) as data_extracao,
            {{ process_null('ano_particao') }} as ano_particao,
            {{ process_null('mes_particao') }} as mes_particao,
            {{ process_null('data_particao') }} as data_particao
        from source
    )


select * from diario_padronizado