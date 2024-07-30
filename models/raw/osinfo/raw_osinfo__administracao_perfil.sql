{{
    config(
        alias="administracao_perfil",
    )
}}


with source as (
      select * from {{ source('osinfo', 'administracao_perfil') }}
),
renamed as (
    select
        {{ adapter.quote("id_perfil") }},
        {{ adapter.quote("nome_perfil") }}

    from source
)
select * from renamed
  