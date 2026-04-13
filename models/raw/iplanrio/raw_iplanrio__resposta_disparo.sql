{{
    config(
        alias="resposta_disparo",
        materialized="table",
        schema="brutos_iplanrio"
    )
}}

select *
from {{ source('iplanrio_intermediario_rmi_conversas', 'resposta_disparo') }}