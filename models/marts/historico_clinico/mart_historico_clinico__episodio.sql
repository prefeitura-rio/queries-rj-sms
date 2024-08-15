{{
    config(
        schema="saude_historico_clinico",
        alias="episodio_assistencial",
        materialized="table",
    )
}}

with 
    vitai as (
        select * from {{ ref("int_historico_clinico__episodio__vitai") }}
    ),
    vitacare as (
        select * from {{ ref("int_historico_clinico__episodio__vitacare") }}
    )
select * from vitai
union all
select * from vitacare
