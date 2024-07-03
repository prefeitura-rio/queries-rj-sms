{{ config(
    alias="paciente",
    materialized='incremental',
    unique_key='gid',
    tags=["vitai_db"]
) }}

with 
recent_events as (
    select *, row_number() over (partition by gid order by data_hora desc) as row_num
    from {{ ref("raw_prontuario_vitai__paciente_eventos_recentes") }}
), 
latest_events as (
    select *
    from recent_events
    where row_num = 1
)
select *
from latest_events
{% if is_incremental() %}
where data_hora >= (SELECT max(data_hora) FROM {{ this }})
{% endif %}
