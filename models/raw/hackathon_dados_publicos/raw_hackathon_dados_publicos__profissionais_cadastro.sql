{{
    config(
        alias="profissionais_cadastro",
        schema="hackathon_dados_publicos",
        materialized="table",
    )
}}

with source as (
      select * from {{ ref('raw_cnes_ftp__profissional') }}
    where ano = 2024 and mes = 1 and sigla_UF = "RJ"
)

select * from source
