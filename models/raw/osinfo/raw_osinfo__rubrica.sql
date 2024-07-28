{{
    config(
        alias="rubrica",
    )
}}


with source as (
      select * from {{ source('osinfo', 'rubrica') }}
),
renamed as (
    select
        {{ adapter.quote("id_rubrica") }},
        {{ adapter.quote("rubrica") }},
        {{ adapter.quote("flg_ativo") }}

    from source
)
select * from renamed
  