{{ 
    config(
        materialized = 'table',
        schema = 'intermediario_cdi',
        alias = 'demanda_interna_externa'
    ) 
}}


with base as (

    select *
    from {{ ref('raw_cdi__demanda_interna_externa') }}

)

select * from base
order by 1