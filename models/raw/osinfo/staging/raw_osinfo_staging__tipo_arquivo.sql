{{
    config(
        alias="tipo_arquivo",
        schema="brutos_osinfo_staging"
    )
}}


with source as (
      select * from {{ source('osinfo', 'tipo_arquivo') }}
),
renamed as (
    select
        {{ adapter.quote("id_tipo_arquivo") }},
        {{ adapter.quote("tipo_servico") }},
        {{ adapter.quote("extensao") }},
        {{ adapter.quote("flg_atividade") }}

    from source
)
select * from renamed
  