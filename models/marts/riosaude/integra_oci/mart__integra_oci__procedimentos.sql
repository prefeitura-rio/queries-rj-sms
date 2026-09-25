{{
    config(
        alias="procedimentos",
        materialized="table",
    )
}}

with

sarah_procedimentos as (
    select *, 'sarah' as fonte
    from {{ref('int__integra_oci__procedimento_sarah')}}
),

vitai_procedimentos as (
    select *, 'vitai' as fonte
    from {{ref('int__integra_oci__procedimento_vitai')}}
),

vitacare_procedimentos as (
    select *, 'vitacare' as fonte
    from {{ref('int__integra_oci__procedimento_vitacare')}}
),

uniao as (
    select * from sarah_procedimentos
    union all
    select * from vitai_procedimentos
    union all
    select * from vitacare_procedimentos
)
select *
from uniao

