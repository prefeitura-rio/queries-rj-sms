{{
    config(
        schema="saude_historico_clinico",
        alias="alergia",
        materialized="table",
    )
}}

with 
    vitai as (
        select
            id_paciente,
            cns,
            cpf,
            alergia
        from {{ ref("int_historico_clinico__alergia__vitai") }},
            unnest(alergias) as alergia
    ),
    vitacare as (
        select
            safe_cast(null as string) as id_paciente,
            safe_cast(null as string) as cns,
            cpf,
            alergia
        from {{ ref("int_historico_clinico__alergia__vitacare") }},
            unnest(alergias) as alergia
    ),
    total as (
        select * from vitai
        union all
        select * from vitacare
    )
select
    id_paciente,
    cns,
    cpf,
    array_agg(alergia) as alergias,
    safe_cast(current_datetime() as datetime) as processed_at
from total
group by id_paciente, cns, cpf

