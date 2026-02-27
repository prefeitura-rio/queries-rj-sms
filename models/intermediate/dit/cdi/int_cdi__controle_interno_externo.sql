{{ 
    config(
        materialized = 'table',
        schema = 'intermediario_cdi',
        alias = 'controle_interno_externo'
    ) 
}}


with base as (

    select *
    from {{ ref('raw_cdi__controle_interno_externo') }}

)

select * from base
