{{
    config(
        alias="administracao_perfil",
        schema="brutos_osinfo_staging"
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
  