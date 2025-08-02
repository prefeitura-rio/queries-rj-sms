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
),
source_deduped as (
    select * 
    from source 
    qualify row_number() over (partition by cpf, id_hci order by data_particao desc)  = 1
)
select * 
from source_deduped
where flag_gemini = '1'
or flag_gemini = '0'