{{
    config(
        alias="cpf",
        materialized="table",
    )
}}

select * from {{ source("brutos_bcadastro", 'cpf') }}