{{
    config(
        alias="paciente",
        materialized="table",
    )
}}

-- dbt run --select raw_prontuario_vitacare__paciente
with
    patients as (
        select *, 'rotineiro' as tipo,
        from {{ ref("base_prontuario_vitacare__paciente_rotineiro") }}
        union all
        select *, 'historico' as tip
        from {{ ref("base_prontuario_vitacare__paciente_historico") }}
    ),
    patients_with_cpf as (select * from patients where cpf is not null)
select *
from patients_with_cpf
