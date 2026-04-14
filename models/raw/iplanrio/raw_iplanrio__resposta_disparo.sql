{{
    config(
        materialized="table",
        schema="brutos_iplanrio",
        alias="resposta_disparo",
        tags=['daily']
    )
}}

select *
from {{ source('iplanrio_intermediario_rmi_conversas', 'resposta_disparo') }}
where lower(nome_hsm) like '%puerpera%'