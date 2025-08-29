{{
    config(
        alias="diario_uniao",
        materialized="table"
    )
}}
-- TODO: apagar esse modelo

with
    source as (
        select *
        from {{ source("brutos_diario_oficial_staging", "diarios_uniao") }}
    ),

    diario_padronizado as (
        select
            {{ process_null('title') }} as titulo,
            parse_date('%d/%m/%Y', published_at) as data_publicacao,
            {{ process_null('signatures') }} as assinaturas,
            {{ process_null('text_title') }} as cabecalho,
            {{ process_null('edition') }} as edicao,
            {{ process_null('section') }} as secao,
            {{ process_null('page') }} as pagina,
            {{ process_null('agency') }} as organizacao_principal,
            trim({{ process_null('text') }}) as texto,
            {{ process_null('url') }} as link,
            {{ process_null('text_title') }} as texto_titulo,
            safe_cast(_extracted_at as timestamp) as data_extracao,
            {{ process_null('ano_particao') }} as ano_particao,
            {{ process_null('mes_particao') }} as mes_particao,
            {{ process_null('data_particao') }} as data_particao
        from source
    )

select *
from diario_padronizado
