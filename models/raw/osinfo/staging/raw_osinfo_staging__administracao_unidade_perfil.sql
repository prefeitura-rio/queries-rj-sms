{{
    config(
        alias="administracao_unidade_perfil",
        schema="brutos_osinfo_staging"
    )
}}

with source as (
      select * from {{ source('osinfo', 'administracao_unidade_perfil') }}
),
renamed as (
    select
        {{ adapter.quote("id_unidade_perfil") }},
        {{ adapter.quote("id_usuario") }},
        {{ adapter.quote("cod_unidade") }},
        {{ adapter.quote("id_perfil") }}

    from source
)
select * from renamed
  