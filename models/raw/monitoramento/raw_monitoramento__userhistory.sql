{{ config(alias="userhistory", tags=["monitoramento"]) }}


with
    source as (
        select *
        from {{ source("gerenciamento__historico_clinico__logs_staging", "userhistory") }}
    )
select *
from source
