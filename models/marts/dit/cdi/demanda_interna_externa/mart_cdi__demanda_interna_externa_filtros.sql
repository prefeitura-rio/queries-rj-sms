{{ 
    config(
        materialized = 'table',
        schema = 'projeto_cdi',
        alias = 'demanda_interna_externa_filtros'
    ) 
}}

with base as (

    select *
    from {{ ref('int_cdi__demanda_interna_externa') }}

),

base_tratada as (

    select
        subsecretaria___setor,
        data_de_entrada,
        case when manifestacao is null then 'Sem informação' else manifestacao end as manifestacao,
        status,
        unidade_ap,
        vencimento_1,
        data_da_ultima_atualizacao,
        orgao_demandante,
        case when tipo_de_demanda is null then 'Sem informação' else tipo_de_demanda end as tipo_de_demanda,
        count(distinct processorio_sei) as total_processos
    from base
    group by 1,2,3,4,5,6,7,8,9

)

select *
from base_tratada
order by total_processos desc
