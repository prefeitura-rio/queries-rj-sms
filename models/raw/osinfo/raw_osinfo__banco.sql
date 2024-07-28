{{
    config(
        alias="banco",
    )
}}

with source as (
      select * from {{ source('osinfo', 'banco') }}
),
renamed as (
    select
        {{ adapter.quote("id_banco") }},
        {{ adapter.quote("cod_banco") }},
        {{ adapter.quote("banco") }},
        {{ adapter.quote("nome_fantasia") }},
        {{ adapter.quote("flg_ativo") }}

    from source
)
select * from renamed
  