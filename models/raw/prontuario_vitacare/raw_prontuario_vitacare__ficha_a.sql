{{
    config(
        alias="ficha_a",
        materialized="table",
    )
}}

-- dbt run --select raw_prontuario_vitacare__paciente
with
    ficha_a as (
        select *, 'rotineiro' as tipo,
        from {{ ref("base_prontuario_vitacare__ficha_a_rotineiro") }}
        union all
        select *, 'historico' as tipo,
        from {{ ref("base_prontuario_vitacare__ficha_a_historico") }}
    )

select *
from ficha_a
