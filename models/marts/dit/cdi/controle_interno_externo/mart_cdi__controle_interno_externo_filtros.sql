{{ 
    config(
        materialized = 'table',
        schema = 'projeto_cdi',
        alias = 'controle_interno_externo_filtros'
    ) 
}}

with base as (

    select *
    from {{ ref('int_cdi__controle_interno_externo') }}

),

base_tratada as (

    select
        subsecretaria___setor,
        data_de_entrada,
        case when manifestacao is null then 'Sem informação' else manifestacao end as manifestacao,
        status,
        unidade_ap,
        vencimento_1,
        case when relator_auditor is null then 'Sem informação ou Sem Relator' else relator_auditor end as relator_auditor,
        data_da_ultima_atualizacao,
        orgao_demandante,
        case when tipo_de_demanda is null then 'Sem informação' else tipo_de_demanda end as tipo_de_demanda,
        count(id) as total_processos -- para a contagem ficar o mais correta possivel
    from base
    group by 1,2,3,4,5,6,7,8,9,10

)

select *
from base_tratada
