{{
    config(
        alias="conta_bancaria",
        schema="brutos_osinfo_staging"
    )
}}


with source as (
      select * from {{ source('osinfo', 'conta_bancaria') }}
),
renamed as (
    select
        {{ adapter.quote("id_conta_bancaria") }},
        {{ adapter.quote("id_agencia") }},
        {{ adapter.quote("codigo_cc") }},
        {{ adapter.quote("digito_cc") }},
        {{ adapter.quote("flg_ativo") }},
        {{ adapter.quote("cod_instituicao") }},
        {{ adapter.quote("cod_tipo") }}

    from source
)
select * from renamed
  