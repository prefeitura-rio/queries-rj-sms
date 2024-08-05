{{
    config(
        alias="secretaria",
        schema="brutos_osinfo_staging"
    )
}}


with source as (
      select * from {{ source('osinfo', 'secretaria') }}
),
renamed as (
    select
        {{ adapter.quote("id_secretaria") }},
        {{ adapter.quote("secretaria") }},
        {{ adapter.quote("sigla") }},
        {{ adapter.quote("regional") }},
        {{ adapter.quote("sigla_regional") }},
        {{ adapter.quote("cod_secretaria") }},
        {{ adapter.quote("flg_regional") }}

    from source
)
select * from renamed
  