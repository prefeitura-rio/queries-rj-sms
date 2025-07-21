{{
    config(
        schema="intermediario_historico_clinico",
        alias="paciente_restrito",
        materialized="table",
    )
}}
with source as (
    select * 
    from {{ source("intermediario_historico_clinico_staging", "paciente_restrito") }}
)
select * 
from source
where flag_gemini = '1'
or flag_gemini = '0'