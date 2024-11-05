{{
    config(
        alias="atividade",
        materialized="table",
    )
}}

with
    events_from_window as (
        select 
            *
        from {{ source("brutos_plataforma_clickup_staging", "atividades_eventos") }}
    ),
    events_ranked_by_freshness as (
        select *, row_number() over (partition by id order by date_updated desc) as rank
        from events_from_window
    ),
    latest_events as (
        select * from events_ranked_by_freshness where rank = 1
    )
select *
from latest_events