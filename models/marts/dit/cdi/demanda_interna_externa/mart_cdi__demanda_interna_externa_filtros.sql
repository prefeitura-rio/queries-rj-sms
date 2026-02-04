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
        manifestacao,
        vencimento_1,
        orgao_demandante,
        tipo_de_demanda,
        count(distinct processorio_sei) as total_processos
    from base
    group by 1,2,3,4,5,6
    order by 7 desc

)

select * from base_tratada