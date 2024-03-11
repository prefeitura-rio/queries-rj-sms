{{
    config(
        alias="sisreg_procedimentos_cadastro",
        schema="hackathon_dados_publicos",
        materialized="table",
    )
}}

with source as (
      select * from {{ source('hackathon_dados_publicos_staging', 'sisreg_procedimentos_cadastro') }}
),
renamed as (
    select
        {{ adapter.quote("procedimento_nome") }},
        {{ adapter.quote("id_procedimento") }},
        {{ adapter.quote("procedimento_especialidade") }},
        {{ adapter.quote("procedimento_tipo") }}

    from source
)
select * from renamed
  