{{
    config(
        alias="estado_entrega",
        schema="brutos_osinfo_staging"
    )
}}


with source as (
      select * from {{ source('osinfo', 'estado_entrega') }}
),
renamed as (
    select
        {{ adapter.quote("cod_estado") }},
        {{ adapter.quote("estado") }},
        {{ adapter.quote("etiqueta") }},
        {{ adapter.quote("detalhe") }}

    from source
)
select * from renamed
  