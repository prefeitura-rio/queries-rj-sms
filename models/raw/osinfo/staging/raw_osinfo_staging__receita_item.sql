{{
    config(
        alias="receita_item",
        schema="brutos_osinfo_staging"
    )
}}


with source as (
      select * from {{ source('osinfo', 'receita_item') }}
),
renamed as (
    select
        {{ adapter.quote("id_receita_item") }},
        {{ adapter.quote("receita_item") }},
        {{ adapter.quote("flg_ativo") }},
        {{ adapter.quote("ordem") }},
        {{ adapter.quote("id_receita_tipo") }}

    from source
)
select * from renamed
  