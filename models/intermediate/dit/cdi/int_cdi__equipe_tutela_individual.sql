{{ 
    config(
        materialized = 'table',
        schema = 'intermediario_cdi',
        alias = 'int_cdi__equipe_tutela_individual_v2'
    ) 
}}

with base as (

    select *
    from {{ ref('raw_cdi__tutela_individual') }}

)

select * from base


