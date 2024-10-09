{{ config(alias="userinfo", tags=["monitoramento"]) }}


with
    source as (
        select *
        from {{ source("gerenciamento__historico_clinico__logs_staging", "userinfo") }}
    )
select *
from source
