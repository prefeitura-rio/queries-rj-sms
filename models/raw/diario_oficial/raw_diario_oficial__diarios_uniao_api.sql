{{
    config(
        alias="diario_uniao_api",
        materialized="table"
    )
}}

with
    source as (
        select *
        from {{ source("brutos_diario_oficial_staging", "diarios_uniao_api") }}
    ),

    diario_padronizado as (
        select
            {{ process_null('title') }} as titulo,
            {{ process_null('id') }} as id,
            {{ process_null('act_id') }} as id_oficio,
            {{ process_null('text_title') }} as texto_titulo,
            parse_date('%d/%m/%Y', published_at) as data_publicacao,
            {{ process_null('agency') }} as organizacao_principal, 
            {{ process_null('number_page') }} as number_page,
            {{ process_null('edition') }} as edicao,
            {{ process_null('section') }} as secao,
            {{ process_null('text') }} as texto,
            {{ process_null('signatures') }} as assinaturas,
            {{ process_null('role') }} as cargo,
            {{ process_null('url') }} as link,
            safe_cast(extracted_at as timestamp) as data_extracao,
            {{ process_null('ano_particao') }} as ano_particao,
            {{ process_null('mes_particao') }} as mes_particao,
            {{ process_null('data_particao') }} as data_particao
        from source
    )

select *
from diario_padronizado
