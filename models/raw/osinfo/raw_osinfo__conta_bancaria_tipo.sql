{{
    config(
        alias="conta_bancaria_tipo",
    )
}}


with source as (
      select * from {{ source('osinfo', 'conta_bancaria_tipo') }}
),
renamed as (
    select
        {{ adapter.quote("id_conta_bancaria_tipo") }},
        {{ adapter.quote("tipo") }},
        {{ adapter.quote("sigla") }}

    from source
)
select * from renamed
  