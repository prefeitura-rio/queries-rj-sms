{{
    config(
        schema="saude_historico_clinico",
        alias="episodio_assistencial",
        materialized="table",
    )
}}

with vitai as (select * from {{ ref("int_historico_clinico__episodio__vitai") }})

select *
from vitai
