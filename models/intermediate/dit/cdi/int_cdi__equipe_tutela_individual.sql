{{ 
    config(
        materialized = 'table',
        schema = 'intermediario_cdi',
        alias = 'equipe_tutela_individual',
        meta={"owner": "karen"}
    ) 
}}

with base as (

    select *
    from {{ ref('raw_cdi__tutela_individual') }}

)

select * from base


