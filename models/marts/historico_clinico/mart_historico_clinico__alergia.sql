{{
    config(
        schema="saude_historico_clinico",
        alias="alergia",
        materialized="table",
    )
}}

with 
    vitai as (
        select * from {{ ref("int_historico_clinico__alergia__vitai") }}
    )

select 
    id_paciente,
    cns,
    cpf,
    alergias
from vitai
