{{ config(alias="userhistory", tags=["monitoramento"]) }}


with
    source as (
        select *
        from {{ source("gerenciamento__historico_clinico__logs_staging", "userhistory") }}
    ),
    
    ranked_by_freshness as (
        select *, 
            row_number() over (partition by id order by timestamp desc) as rank
        from source
    ),
    
    -- Seleciona apenas os eventos mais recentes de cada grupo
    latest as (
        select * 
        from ranked_by_freshness 
        where rank = 1
    )
select *
from latest
