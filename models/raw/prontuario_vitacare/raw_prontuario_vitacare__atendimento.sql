
{{
    config(
        alias="atendimento",
        materialized="table",
        cluster_by="cpf",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day"
    }
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
        select *
        from atendimentos
        qualify row_number() over (partition by gid order by updated_at desc) = 1
    )
select *
from atendimentos
