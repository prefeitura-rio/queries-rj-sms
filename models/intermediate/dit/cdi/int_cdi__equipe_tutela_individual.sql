{{ 
    config(
        materialized = 'table',
        schema = 'intermediario_cdi',
        alias = 'equipe_tutela_individual'
    ) 
}}

with base as (

    select *
    from {{ ref('raw_cdi__tutela_individual') }}

)

select * from base


