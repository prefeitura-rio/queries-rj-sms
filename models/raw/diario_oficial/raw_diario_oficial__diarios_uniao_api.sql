{{
    config(
        alias="diario_uniao_api",
        materialized="table",
        tags=["cdi_vps"],
        partition_by={
            "field": "data_particao",
            "data_type": "date"
        }
    )
}}

with
    source as (
        select *
        from {{ source("brutos_diario_oficial_staging", "diarios_uniao_api") }}
    ),

    diario_padronizado as (
        select
            cast({{ process_null('title') }} as string) as titulo,
            cast({{ process_null('id') }} as string) as id,
            cast({{ process_null('act_id') }} as string) as id_oficio,
            cast({{ process_null('text_title') }} as string) as texto_titulo,
            parse_date('%d/%m/%Y', published_at) as data_publicacao,
            cast({{ process_null('agency') }} as string) as organizacao_principal, 
            cast({{ process_null('number_page') }} as string) as number_page,
            cast({{ process_null('edition') }} as string) as edicao,
            cast({{ process_null('section') }} as string) as secao,
            cast({{ process_null('text') }} as string) as texto,
            cast({{ process_null('signatures') }} as string) as assinaturas,
            cast({{ process_null('role') }} as string) as cargo,
            cast({{ process_null('url') }} as string) as link,
            safe_cast(extracted_at as timestamp) as data_extracao,
            cast({{ process_null('ano_particao') }} as int64) as ano_particao,
            cast({{ process_null('mes_particao') }} as int64) as mes_particao,
            cast({{ process_null('data_particao') }} as date) as data_particao
        from source
    )

select *
from diario_padronizado
