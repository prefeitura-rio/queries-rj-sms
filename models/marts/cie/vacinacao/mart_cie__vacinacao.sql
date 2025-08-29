{{
    config(
        alias="vacinacao",
        materialized="table",
    )
}}

with
    source as (
        select * from {{ ref("raw_prontuario_vitacare__vacinacao") }}
    )

select * 
from source