{{
    config(
        alias="receita_dados",
    )
}}


with source as (
      select * from {{ source('osinfo', 'receita_dados') }}
),
renamed as (
    select
        {{ adapter.quote("id_receita_dados") }},
        {{ adapter.quote("cod_unidade") }},
        {{ adapter.quote("id_item") }},
        {{ adapter.quote("referencia_mes") }},
        {{ adapter.quote("referencia_ano") }},
        {{ adapter.quote("valor") }},
        {{ adapter.quote("flg_ativo") }},
        {{ adapter.quote("id_instrumento_contratual") }},
        {{ adapter.quote("id_termo_aditivo") }},
        {{ adapter.quote("id_conta_bancaria") }}

    from source
)
select * from renamed
  