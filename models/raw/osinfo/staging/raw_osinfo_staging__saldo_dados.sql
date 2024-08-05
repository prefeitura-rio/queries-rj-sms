{{
    config(
        alias="saldo_dados",
        schema="brutos_osinfo_staging"
    )
}}

with source as (
      select * from {{ source('osinfo', 'saldo_dados') }}
),
renamed as (
    select
        {{ adapter.quote("id_saldo_dados") }},
        {{ adapter.quote("id_saldo_item") }},
        {{ adapter.quote("referencia_mes_receita") }},
        {{ adapter.quote("referencia_ano_receita") }},
        {{ adapter.quote("valor") }},
        {{ adapter.quote("flg_ativo") }},
        {{ adapter.quote("id_instrumento_contratual") }},
        {{ adapter.quote("id_conta_bancaria") }},
        {{ adapter.quote("arq_img_ext") }}

    from source
)
select * from renamed
  