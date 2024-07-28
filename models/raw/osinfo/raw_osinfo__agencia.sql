{{
    config(
        alias="agencia",
    )
}}
with source as (
      select * from {{ source('osinfo', 'agencia') }}
),
renamed as (
    select
        {{ adapter.quote("id_agencia") }},
        {{ adapter.quote("id_banco") }},
        {{ adapter.quote("numero_agencia") }},
        {{ adapter.quote("digito") }},
        {{ adapter.quote("agencia") }},
        {{ adapter.quote("flg_ativo") }}

    from source
)
select * from renamed
  