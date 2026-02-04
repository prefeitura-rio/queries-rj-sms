{{ 
    config(
        materialized = 'table',
        schema = 'intermediario_cdi',
        alias = 'demanda_interna_externa'
    ) 
}}

with base as (

    select *
    from {{ ref('raw_cdi__controle_interno_externo') }}

)

select * from base