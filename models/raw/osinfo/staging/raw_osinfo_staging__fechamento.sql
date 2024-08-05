{{
    config(
        alias="fechamento",
        schema="brutos_osinfo_staging"
    )
}}


with source as (
      select * from {{ source('osinfo', 'fechamento') }}
),
renamed as (
    select
        {{ adapter.quote("id_fechamento") }},
        {{ adapter.quote("mes_referencia") }},
        {{ adapter.quote("ano_referencia") }},
        {{ adapter.quote("cod_usuario") }},
        {{ adapter.quote("data_inclusao") }},
        {{ adapter.quote("id_instrumento_contratual") }},
        {{ adapter.quote("data_limite") }},
        {{ adapter.quote("cod_instituicao") }},
        {{ adapter.quote("cod_estado_entrega") }},
        {{ adapter.quote("cod_tipo_entrega") }}

    from source
)
select * from renamed
  