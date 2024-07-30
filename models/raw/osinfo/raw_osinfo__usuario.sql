{{
    config(
        alias="usuario",
    )
}}

with source as (
      select * from {{ source('osinfo', 'usuario') }}
),
renamed as (
    select
        {{ adapter.quote("cod_usuario") }},
        {{ adapter.quote("cod_unidade") }},
        {{ adapter.quote("login") }},
        {{ adapter.quote("nome") }},
        {{ adapter.quote("data_cadastro") }},
        {{ adapter.quote("data_atualizacao") }},
        {{ adapter.quote("data_exclusao") }},
        {{ adapter.quote("flg_excluido") }},
        {{ adapter.quote("cargo") }}

    from source
)
select * from renamed
  