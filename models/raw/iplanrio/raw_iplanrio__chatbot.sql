{{
    config(
        materialized="table",
        schema="brutos_iplanrio",
        alias="chatbot",
        tags=['daily']
    )
}}

select *
from {{ source('iplanrio_rmi_conversas', 'chatbot') }}
where lower(hsm.nome_hsm) like '%puerpera%'