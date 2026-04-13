{{
    config(
        alias="chatbot",
        materialized="table",
        schema="brutos_iplanrio"
    )
}}

select *
from {{ source('iplanrio_rmi_conversas', 'chatbot') }}