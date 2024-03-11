{{
    config(
        alias="estabelecimentos_cadastro",
        schema="hackathon_dados_publicos",
        materialized="table",
    )
}}

with source as (
      select * from {{ ref('dim_estabelecimento') }}
)

select * from source
