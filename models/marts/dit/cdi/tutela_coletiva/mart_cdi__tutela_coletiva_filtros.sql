{{ 
    config(
        materialized = 'table',
        schema = 'projeto_cdi',
        alias = 'tutela_coletiva_filtros',
        meta={"owner": "karen"}
    ) 
}}

with base as (

    select *
    from {{ ref('int_cdi__tutela_coletiva') }}

),

filtros as (

    select
        case when 
            area is null then 'Não Aplicável' 
            else area 
        end as area,
        assunto,
        data_entrada,
        case
            
            when upper(orgao) like 'MPF%' then 'MPF'

            when upper(orgao) like 'MPRJ%'
            or upper(orgao) like 'PJTCPICAP%'
            or upper(orgao) like 'CAO%' then 'MPRJ'

            when upper(orgao) like '%COSAU%' then 'COSAU'

            when upper(orgao) like 'DPU%' then 'DPU'

            when upper(orgao) like 'DGH%' then 'DGH'

            else 'Outros'
        end as grupo_orgao,

        case 
            when ic is null then 'Não Aplicável' 
            else ic 
        end as ic,
        case 
            when orgao is null then 'Sem Informação' 
            else orgao 
        end as orgao,
        reiteracoes,
        sintese_solicitacao,
        status,

        count(processo_rio) as total_requisicoes

    from base

    group by
        area,
        assunto,
        data_entrada,
        grupo_orgao,
        ic,
        orgao,
        reiteracoes,
        sintese_solicitacao,
        status

)

select *
from filtros