{{
    config(
        alias="atendimento",
        materialized="table",
    )
}}


with
    atendimentos as (
        select *, 'rotineiro' as origem,
        from {{ ref("base_prontuario_vitacare__atendimento_rotineiro") }}
        union all
        select *, 'historico' as origem
        from {{ ref("base_prontuario_vitacare__atendimento_historico") }}
    ),
    atendimentos_ranqueados as (
        select
            *,
            row_number() over (partition by gid order by updated_at desc) as rank
        from atendimentos
    )
select *
from atendimentos
