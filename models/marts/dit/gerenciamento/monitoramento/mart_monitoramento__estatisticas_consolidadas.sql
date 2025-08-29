{{
    config(
        schema="gerenciamento__monitoramento",
        alias="estatisticas_consolidadas",
        materialized="table",
    )
}}

with
    juncao as (
        select * from {{ ref('int_monitoramento__historico_clinico__vitacare') }}
            union all
        select * from {{ ref('int_monitoramento__historico_clinico__vitai') }}
            union all
        select * from {{ ref('int_monitoramento__farmacia_digital__vitacare') }}
            union all
        select * from {{ ref('int_monitoramento__farmacia_digital__tpc') }}
    )
select *
from juncao
where data_ingestao >= '2024-12-01'
order by id