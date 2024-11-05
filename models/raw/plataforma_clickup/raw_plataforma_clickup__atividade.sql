{{
    config(
        alias="atividade",
        materialized="table",
    )
}}

with
    events_from_window as (
        select 
            JSON_VALUE(id, '$') as task_id,
            TIMESTAMP_MILLIS(SAFE_CAST(JSON_VALUE(date_updated, '$') as int64)) as date_updated,
            * except (date_updated)
        from {{ source("brutos_plataforma_clickup_staging", "atividades_eventos") }}
    ),
    events_ranked_by_freshness as (
        select *, row_number() over (partition by task_id order by date_updated desc) as rank
        from events_from_window
    ),
    latest_events as (
        select * from events_ranked_by_freshness where rank = 1
    ),

    dim_assignees as (
        select 
            JSON_VALUE(latest_events.id, '$') as task_id,
            array_agg(
                JSON_VALUE(assignee_json, '$.username')
            ) as assignees,
        from latest_events,
            UNNEST(json_extract_array(latest_events.assignees)) as assignee_json
        group by 1
    ),

    dim_tags as (
        select 
            JSON_VALUE(latest_events.id, '$') as task_id,
            array_agg(
                JSON_VALUE(tag_json, '$.name')
            ) as tags,
        from latest_events,
            UNNEST(json_extract_array(latest_events.tags)) as tag_json
        group by 1
    ),

    fact_task as (
        select
            task_id,
            JSON_VALUE(latest_events.name, '$') as task_name,
            JSON_VALUE(latest_events.text_content, '$') as text_content,

            TIMESTAMP_MILLIS(SAFE_CAST(JSON_VALUE(latest_events.date_created, '$') as int64)) as date_created,
            TIMESTAMP_MILLIS(SAFE_CAST(JSON_VALUE(latest_events.date_closed, '$') as int64)) as date_closed,
            TIMESTAMP_MILLIS(SAFE_CAST(JSON_VALUE(latest_events.date_done, '$') as int64)) as date_done,

            JSON_VALUE(latest_events.creator, '$.username') as creator_username,
            dim_assignees.assignees,
            dim_tags.tags,

            JSON_VALUE(latest_events.list, '$.id') as list_id,
            JSON_VALUE(latest_events.list, '$.name') as list_name,
            JSON_VALUE(latest_events.url, '$') as task_url,

            struct(
                date_updated as updated_at,
                SAFE_CAST(datalake_loaded_at as timestamp) as loaded_at
            ) as metadata
        from latest_events
            left join dim_assignees using (task_id)
            left join dim_tags using (task_id)
    )
select *
from fact_task
order by metadata.updated_at asc