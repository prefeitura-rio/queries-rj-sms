{{
    config(
        schema = 'intermediario_cdi',
        alias = 'demanda_interna_externa_derivados',  
        materialized = 'table'
    )
}}


with base as (

    select *
    from {{ ref('raw_cdi__demanda_interna_externa_derivados') }}

)

select * from base
