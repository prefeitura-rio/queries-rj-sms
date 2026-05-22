{{
    config(
        materialized="view",
        schema="brutos_iplanrio",
        alias="chatbot"
    )
}}

select *
from {{ source('iplanrio_rmi_conversas', 'chatbot') }}
where lower(hsm.nome_hsm) like '%puerpera%'