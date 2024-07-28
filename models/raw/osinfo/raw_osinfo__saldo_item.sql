{{
    config(
        alias="saldo_item",
    )
}}


with source as (
      select * from {{ source('osinfo', 'saldo_item') }}
),
renamed as (
    select
        {{ adapter.quote("id_saldo_item") }},
        {{ adapter.quote("saldo_item") }},
        {{ adapter.quote("flg_ativo") }},
        {{ adapter.quote("ordem") }},
        {{ adapter.quote("id_saldo_tipo") }}

    from source
)
select * from renamed
  