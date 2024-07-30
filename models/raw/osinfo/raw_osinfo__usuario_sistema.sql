{{
    config(
        alias="usuario_sistema",
    )
}}

with source as (
      select * from {{ source('osinfo', 'usuario_sistema') }}
),
renamed as (
    select
        {{ adapter.quote("id_usuario_sistema") }},
        {{ adapter.quote("cod_usuario") }},
        {{ adapter.quote("id_sistema") }},
        {{ adapter.quote("id_perfil") }},
        {{ adapter.quote("data_inicial") }},
        {{ adapter.quote("data_final") }}

    from source
)
select * from renamed
  