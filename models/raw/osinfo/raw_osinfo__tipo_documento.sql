{{
    config(
        alias="tipo_documento",
    )
}}

with source as (
      select * from {{ source('osinfo', 'tipo_documento') }}
),
renamed as (
    select
        {{ adapter.quote("id_tipo_documento") }},
        {{ adapter.quote("tipo_documento") }},
        {{ adapter.quote("documento") }}

    from source
)
select * from renamed
  