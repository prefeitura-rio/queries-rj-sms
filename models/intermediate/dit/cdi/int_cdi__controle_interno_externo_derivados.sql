{{
    config(
        schema = 'intermediario_cdi',
        alias = 'controle_interno_externo_derivados',  
        materialized = 'table'
    )
}}


with base as (

    select *
    from {{ ref('raw_cdi__controle_interno_externo_derivados') }}

)

select * from base
